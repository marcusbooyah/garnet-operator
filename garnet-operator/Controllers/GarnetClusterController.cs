using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Garnet.client;

using GarnetOperator.Models;
using GarnetOperator.Util;

using k8s;
using k8s.Autorest;
using k8s.Models;

using Microsoft.Extensions.Logging;

using Neon.Common;
using Neon.Diagnostics;
using Neon.K8s;
using Neon.K8s.Core;
using Neon.Operator;
using Neon.Operator.Attributes;
using Neon.Operator.Controllers;
using Neon.Operator.Finalizers;
using Neon.Operator.Rbac;
using Neon.Tasks;
using OpenTelemetry;

namespace GarnetOperator
{
    /// <summary>
    /// Controller for managing GarnetCluster resources.
    /// </summary>
    [RbacRule<V1Pod>(Scope = EntityScope.Cluster, Verbs = RbacVerb.All, SubResources = "status")]
    [RbacRule<V1Service>(Scope = EntityScope.Cluster, Verbs = RbacVerb.All, SubResources = "status")]
    [RbacRule<V1ConfigMap>(Scope = EntityScope.Cluster, Verbs = RbacVerb.All, SubResources = "status")]
    [RbacRule<V1PodDisruptionBudget>(Scope = EntityScope.Cluster, Verbs = RbacVerb.All, SubResources = "status")]
    [RbacRule<V1alpha1GarnetCluster>(Scope = EntityScope.Cluster, Verbs = RbacVerb.All, SubResources = "status")]
    public class GarnetClusterController : ResourceControllerBase<V1alpha1GarnetCluster>
    {
        private readonly IKubernetes                              k8s;
        private readonly IFinalizerManager<V1alpha1GarnetCluster> finalizerManager;
        private readonly ILogger<GarnetClusterController>         logger;
        private readonly GarnetHelper                             garnetHelper;
        
        private static TimeSpan RequeueDelay = TimeSpan.FromSeconds(20);

        private V1alpha1GarnetCluster cluster;
        private Dictionary<string, V1Pod> clusterPods;
        private IEnumerable<GarnetNode> primaries => cluster.Status.Cluster.Nodes.Values.Where(n => n.Role == GarnetRole.Primary);
        private IEnumerable<GarnetNode> replicas => cluster.Status.Cluster.Nodes.Values.Where(n => n.Role == GarnetRole.Replica);
        private IEnumerable<GarnetNode> leaving => cluster.Status.Cluster.Nodes.Values.Where(n => n.Role == GarnetRole.Leaving);
        private IEnumerable<GarnetNode> promoting => cluster.Status.Cluster.Nodes.Values.Where(n => n.Role == GarnetRole.Promoting);
        private IEnumerable<GarnetNode> orphans => cluster.Status.Cluster.Nodes.Values.Where(n => n.Role == GarnetRole.None);
        private IEnumerable<GarnetNode> allNodes => cluster.Status.Cluster.Nodes.Values;
        private string headlessServiceName => (cluster.Spec.ServiceName ?? cluster.Name()) + "-headless";

        private Cluster newCluster;
        private TimeSpan? requeueDelay = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="GarnetClusterController"/> class.
        /// </summary>
        /// <param name="k8s">The Kubernetes client.</param>
        /// <param name="finalizerManager">The finalizer manager.</param>
        /// <param name="logger">The logger.</param>
        public GarnetClusterController(
            IKubernetes                              k8s,
            IFinalizerManager<V1alpha1GarnetCluster> finalizerManager,
            GarnetHelper                             garnetHelper,
            ILogger<GarnetClusterController>         logger = null)
        {
            this.k8s              = k8s;
            this.finalizerManager = finalizerManager;
            this.garnetHelper     = garnetHelper;
            this.logger           = logger;
        }

        /// <inheritdoc/>
        public override async Task<ResourceControllerResult> ReconcileAsync(V1alpha1GarnetCluster resource, CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();
            
            cluster = resource;

            logger?.LogInformationEx(() => $"Reconciling: {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");

            await InitializeStatusAsync(cancellationToken);
            clusterPods = await GetClusterPodsAsync(cancellationToken);

            var tasks = new List<Task>()
            {
                ReconcileConfigMapAsync(cancellationToken),
                ReconcileServicesAsync(cancellationToken)
            };

            await Task.WhenAll(tasks);

            await ReconcileClusterAsync(cancellationToken);

            await GetShardsAsync(cancellationToken);

            await RebalanceClusterAsync(cancellationToken);

            logger?.LogInformationEx(() => $"Reconciled: {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");

            return ResourceControllerResult.Ok();
        }

        internal async Task InitializeStatusAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            if (cluster.Status != null)
            {
                return;
            }

            cluster.Status = new V1alpha1GarnetClusterStatus()
            {
                Cluster = new Cluster()
                {
                    NumberOfReplicasPerPrimary = new Dictionary<string, int>()
                },
                Conditions = new List<V1Condition>(),
                StartTime = DateTime.UtcNow,
            };

            var meta = typeof(V1alpha1GarnetCluster).GetKubernetesTypeMetadata();

            cluster = await k8s.CustomObjects.ReplaceNamespacedCustomObjectStatusAsync(
                @object:            cluster,
                namespaceParameter: cluster.Namespace(),
                cancellationToken:  cancellationToken);
        }


        internal async Task GetShardsAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            var shards = await garnetHelper.GetShardsAsync(primaries.First());

            foreach (var shard in shards)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var node = shard.Nodes.Where(n => n.Role == GarnetRole.Primary).FirstOrDefault();

                if (node == null)
                {
                    continue;
                }

                cluster.Status.Cluster.Nodes.Where(n => n.Value.Id == node.Id).FirstOrDefault().Value.Slots = shard.Slots;

                await SaveStatusAsync(cancellationToken);
            }

        }

        internal async Task<ResourceControllerResult> ReconcileClusterAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            var needsMorePods = NeedsMorePods();
            var needsLessPods = NeedsLessPods();

            await WaitForPodReadinessAsync(cancellationToken);

            if (needsMorePods)
            {
                await ScaleUpAsync(cancellationToken);
                await WaitForPodReadinessAsync(cancellationToken);
            }
            else if (needsLessPods)
            {
                await ScaleDownAsync(cancellationToken);
                await WaitForPodReadinessAsync(cancellationToken);
            }

            await ConfigureClusterAsync(cancellationToken);

            await cluster.SetConditionAsync(
                k8s:               k8s,
                type:              Constants.Conditions.Scaling,
                status:            Constants.Conditions.StatusFalse,
                reason:            string.Empty,
                message:           string.Empty,
                cancellationToken: cancellationToken);

            return ResourceControllerResult.Ok();
        }

        internal async Task RebalanceClusterAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            var newNodes = KubernetesHelper.JsonClone(primaries.ToList());

            var numSlices    = newNodes.Count();
            var slotsPerNode = Constants.Redis.Slots / numSlices;

            for (int i = 0; i < numSlices; i++)
            {
                var start = (i) * slotsPerNode;
                var end   = (1 + i) * slotsPerNode;

                if (i == numSlices - 1)
                {
                    end = Constants.Redis.Slots - 1;
                }

                newNodes[i].Slots = new List<int> { start, end };
            }

            var slotsToMigrate = new Dictionary<string, Dictionary<string, SlotMigration>>();

            foreach (var node in newNodes)
            {
                if (cluster.Status.Cluster.Nodes.Values.Where(n => n.Id == node.Id).FirstOrDefault()?.Slots?.IsEmpty() == true)
                {
                    var client = await garnetHelper.CreateClientAsync(node);

                    await client.AddSlotsRangeAsync(
                        address: node.Address,
                        port:    node.Port,
                        start:   node.Slots[0],
                        end:     node.Slots[1]);

                    continue;
                }

                foreach (var slot in node.GetSlots())
                {
                    if (slot >= node.Slots.First() && slot <= node.Slots.Last())
                    {
                        continue;
                    }

                    var toNode = newNodes.Where(n => slot >= n.Slots.First() && slot <= n.Slots.Last()).FirstOrDefault();

                    if (toNode == null)
                    {
                        continue;
                    }

                    slotsToMigrate[node.Id] ??= new Dictionary<string, SlotMigration>();

                    if (slotsToMigrate[node.Id].ContainsKey(toNode.Id))
                    {
                        slotsToMigrate[node.Id][toNode.Id] = new SlotMigration()
                        {
                            FromId = node.Id,
                            ToNode = toNode,
                            Slots = new HashSet<int>()
                        };
                    }

                    slotsToMigrate[node.Id][toNode.Id].Slots.Add(slot);
                }
            }

            foreach (var batch in slotsToMigrate)
            {
                var client = await garnetHelper.CreateClientAsync(newNodes.Where(n => n.Id == batch.Key).First());

                foreach (var toNode in batch.Value)
                {
                    foreach (var r in toNode.Value.GetSlotRanges())
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        await client.MigrateSlotsRangeAsync(
                            address: toNode.Value.ToNode.Address,
                            port:    toNode.Value.ToNode.Port,
                            start:   r.Min,
                            end:     r.Max);
                    }
                }
                
            }
        }

        internal async Task ConfigureClusterAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            GarnetClient clusterClient = null;

            if (primaries.IsEmpty())
            {
                // cluster not initialized, create
                await cluster.SetConditionAsync(
                    k8s: k8s,
                    type: Constants.Conditions.Initializing,
                    status: Constants.Conditions.StatusTrue,
                    reason: "Cluster is being initialized",
                    message: "Cluster is being initialized",
                    cancellationToken: cancellationToken);

                var pod = clusterPods.First().Value;

                clusterClient = await garnetHelper.CreateClientAsync(pod, Constants.Ports.Redis);

                var id = await clusterClient.MyIdAsync();



                cluster.Status.Cluster.Nodes[pod.Uid()].Id                   = id;
                cluster.Status.Cluster.Nodes[pod.Uid()].Role                 = GarnetRole.Primary;
                cluster.Status.Cluster.NumberOfReplicasPerPrimary[pod.Uid()] = 0;
                cluster.Status.LastEpoch = 1;

                await clusterClient.SetConfigEpochAsync(1);

                await SaveStatusAsync(cancellationToken);
            }

            clusterClient ??= await garnetHelper.CreateClientAsync(primaries.First());

            if (cluster.Status.Conditions.Any(c => c.Type == Constants.Conditions.Initializing
                && c.Status == Constants.Conditions.StatusTrue))
            {
                while (promoting.Count() + primaries.Count() < cluster.Spec.NumberOfPrimaries)
                {
                    var pod    = orphans.First();
                    var client = await garnetHelper.CreateClientAsync(pod);

                    await client.SetConfigEpochAsync(cluster.Status.LastEpoch.Value + 1);

                    cluster.Status.Cluster.Nodes[pod.PodUid].Role = GarnetRole.Promoting;
                    cluster.Status.LastEpoch++;

                    await SaveStatusAsync(cancellationToken);
                }

                cluster.Status.LastEpoch = null;

                await cluster.SetConditionAsync(
                    k8s: k8s,
                    type: Constants.Conditions.Initializing,
                    status: Constants.Conditions.StatusFalse,
                    reason: "Cluster is being initialized",
                    message: "Cluster is being initialized",
                    cancellationToken: cancellationToken);


                await SaveStatusAsync(cancellationToken);
            }

            while (primaries.Count() < cluster.Spec.NumberOfPrimaries)
            {
                cancellationToken.ThrowIfCancellationRequested();

                GarnetNode pod = null;
                if (promoting.Any())
                {
                    pod = promoting.First();

                    var replicaClient = await garnetHelper.CreateClientAsync(pod);
                    var resp          = await replicaClient.DetachReplicaAsync();

                    await clusterClient.MeetAsync(pod.Address, Constants.Ports.Redis);
                }
                else
                {
                    pod = orphans.First();
                    var resp = await clusterClient.MeetAsync(pod.Address);
                }

                if (pod == null)
                {
                    return;
                }

                clusterClient = await garnetHelper.CreateClientAsync(pod);

                var id = await clusterClient.MyIdAsync();

                cluster.Status.Cluster.Nodes[pod.PodUid].Id                   = id;
                cluster.Status.Cluster.Nodes[pod.PodUid].Role                 = GarnetRole.Primary;
                cluster.Status.Cluster.NumberOfReplicasPerPrimary[pod.PodUid] = 0;

                await SaveStatusAsync(cancellationToken);
            }

            var numReplicas = cluster.Spec.ReplicationFactor * cluster.Spec.NumberOfPrimaries;

            while (replicas.Count() < numReplicas)
            {
                if (orphans.Any())
                {
                    var pod  = orphans.First();

                    var primaryUid      = cluster.Status.Cluster.NumberOfReplicasPerPrimary.OrderBy(p => p.Value).FirstOrDefault().Key;
                    var primaryReplicas = cluster.Status.Cluster.NumberOfReplicasPerPrimary[primaryUid];
                    var primary         = cluster.Status.Cluster.Nodes[primaryUid];

                    var replicaClient = await garnetHelper.CreateClientAsync(pod);
                    var replicaId     = await replicaClient.MyIdAsync();

                    var primaryClient = await garnetHelper.CreateClientAsync(primary);

                    await primaryClient.MeetAsync(pod.Address, pod.Port);

                    await replicaClient.ReplicateAsync(primary.Id);

                    cluster.Status.Cluster.NumberOfReplicasPerPrimary[primaryUid]  = primaryReplicas + 1;
                    cluster.Status.Cluster.Nodes[pod.PodUid].Id                    = replicaId;
                    cluster.Status.Cluster.Nodes[pod.PodUid].Role                  = GarnetRole.Replica;
                    cluster.Status.Cluster.Nodes[pod.PodUid].PrimaryId             = primaryUid;

                    await SaveStatusAsync(cancellationToken);
                }
            }
        }

        internal async Task SaveStatusAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            var status = KubernetesHelper.JsonClone(cluster.Status);

            cluster = await k8s.CustomObjects.GetNamespacedCustomObjectAsync<V1alpha1GarnetCluster>(
                name:               cluster.Name(),
                namespaceParameter: cluster.Namespace(),
                cancellationToken:  cancellationToken);

            cluster.Status = status;

            cluster = await k8s.CustomObjects.ReplaceNamespacedCustomObjectStatusAsync(
                @object:            cluster,
                namespaceParameter: cluster.Namespace(),
                cancellationToken:  cancellationToken);
        }

        internal async Task ScaleDownAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            await cluster.SetConditionAsync(
                k8s:               k8s,
                type:              Constants.Conditions.Scaling,
                status:            Constants.Conditions.StatusTrue,
                reason:            Constants.Conditions.ScalingDownReason,
                message:           Constants.Conditions.ScalingDownMessage,
                cancellationToken: cancellationToken);

            var primariesToDelete = primaries.Count() - cluster.Spec.NumberOfPrimaries;
            var primaryHosts      = primaries.Select(p => p.NodeName).ToHashSet();
            var podsPerHost       = primariesToDelete / primaryHosts.Count();

            while (primaries.Count() - cluster.Spec.NumberOfPrimaries > 0)
            {
                var pod = primaries.GroupBy(
                    p => p.NodeName,
                    p => p,
                    (key, g) => new
                    {
                        Node  = key,
                        Pods  = g.ToList(),
                        Count = g.Count()
                    })
                    .OrderByDescending(r => r.Count)
                    .FirstOrDefault()
                    .Pods
                    .FirstOrDefault();

                cluster.Status.Cluster.Nodes[pod.PodUid].Role = GarnetRole.Leaving;

                await SaveStatusAsync(cancellationToken);
            }

            var numReplicas     = cluster.Spec.ReplicationFactor * cluster.Spec.NumberOfPrimaries;
            var numPodsRequired = NumPodsRequired();

            while (replicas.Where(n => n.Role != GarnetRole.Leaving).Count() > numReplicas)
            {
                var pod = replicas
                    .Where(n => n.Role != GarnetRole.Leaving)
                    .GroupBy(
                    p => p.NodeName,
                    p => p,
                    (key, g) => new
                    {
                        Node  = key,
                        Pods  = g.ToList(),
                        Count = g.Count()
                    })
                    .OrderByDescending(r => r.Count)
                    .FirstOrDefault()
                    .Pods
                    .FirstOrDefault();

                if (allNodes.Where(n => n.Role != GarnetRole.Leaving).Count() > numPodsRequired)
                {
                    cluster.Status.Cluster.Nodes[pod.PodUid].Role = GarnetRole.Leaving;
                }
                else
                {
                    cluster.Status.Cluster.Nodes[pod.PodUid].Role = GarnetRole.Promoting;
                }

                await SaveStatusAsync(cancellationToken);
            }

            await RemoveLeavingAsync(cancellationToken);
        }

        internal async Task RemoveLeavingAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            foreach (var leavingNode in leaving)
            {
                foreach (var clusterNode in allNodes.Where(p => p.PodUid != leavingNode.PodUid))
                {
                    var client = await garnetHelper.CreateClientAsync(clusterNode);

                    await client.ForgetAsync(leavingNode.Id);
                }

                try
                {
                    await k8s.CoreV1.DeleteNamespacedPodAsync(
                        name:               leavingNode.PodName,
                        namespaceParameter: leavingNode.Namespace,
                        cancellationToken:  cancellationToken);
                }
                catch (HttpOperationException e) when (e.Response.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    logger?.LogInformationEx(() => "Pod has alraedy been deleted");
                }
                catch (Exception e)
                {
                    logger?.LogErrorEx(e);
                    continue;
                }

                cluster.Status.Cluster.TryRemoveNode(leavingNode.PodUid);

                await SaveStatusAsync(cancellationToken);
            }
        }

        internal async Task DetachReplicaAsync(GarnetNode node, V1alpha1GarnetCluster resource, CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            foreach (var clusterNode in resource.Status.Cluster.Nodes.Values.Where(n => n.Address != node.Address))
            {
                var garnet = await garnetHelper.CreateClientAsync(clusterNode);

                if (clusterNode.Id.IsNullOrEmpty())
                {
                    clusterNode.Id = await garnet.MyIdAsync();
                }

                await garnet.ForgetAsync(clusterNode.Id);
            }
        }

        internal async Task ScaleUpAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            await cluster.SetConditionAsync(
                k8s:               k8s,
                type:              Constants.Conditions.Scaling,
                status:            Constants.Conditions.StatusTrue,
                reason:            Constants.Conditions.ScalingUpReason,
                message:           Constants.Conditions.ScalingUpMessage,
                cancellationToken: cancellationToken);

            var numPodsNeeded = NumPodsRequired() - clusterPods?.Count() ?? 0;
            var spec = CreatePodSpec();

            for (int i = 0; i < numPodsNeeded; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var pod = new V1Pod().Initialize();

                pod.Metadata.EnsureLabels();
                pod.Metadata.EnsureAnnotations();
                    
                pod.Metadata.Name              = cluster.CreatePodName();
                pod.Metadata.NamespaceProperty = cluster.Metadata.NamespaceProperty;

                pod.Metadata.Labels.AddRange(cluster.Spec.AdditionalLabels);
                pod.Metadata.SetOwnerReference(cluster.MakeOwnerReference());
                pod.Metadata.Labels.Add(Constants.Labels.ManagedByKey, Constants.OperatorName);
                pod.Metadata.Labels.Add(Constants.Labels.ClusterId, cluster.Uid());

                pod.Spec = spec;

                pod = await k8s.CoreV1.CreateNamespacedPodAsync(
                    body:               pod,
                    namespaceParameter: cluster.Metadata.NamespaceProperty,
                    cancellationToken:  cancellationToken);

                clusterPods.Add(pod.Uid(), pod);

                cluster.Status.Cluster.Nodes.Add(
                    pod.Uid(),
                    new GarnetNode()
                    {
                        Role      = GarnetRole.None,
                        Port      = Constants.Ports.Redis,
                        PodName   = pod.Metadata.Name,
                        Zone      = pod.Spec.NodeName,
                        PodUid    = pod.Uid(),
                        NodeName  = pod.Spec.NodeName,
                        Namespace = cluster.Namespace(),
                    });
            }
        }

        internal async Task WaitForPodReadinessAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            if (clusterPods.Values.All(p => p.Status.ContainerStatuses?.All(cs => cs.Ready == true) == true))
            {
                foreach (var pod in clusterPods)
                {
                    if (!cluster.Status.Cluster.Nodes.ContainsKey(pod.Key))
                    {
                        cluster.Status.Cluster.Nodes.Add(pod.Key, new GarnetNode()
                        {
                            Address   = cluster.CreatePodAddress(pod.Value),
                            Namespace = cluster.Namespace(),
                            NodeName  = pod.Value.Spec.NodeName,
                            PodName   = pod.Value.Name(),
                            PodUid    = pod.Value.Uid(),
                            Port      = Constants.Ports.Redis,
                            Role      = GarnetRole.None,
                            Zone      = pod.Value.Spec.NodeName
                        });
                    }
                }

                return;
            }

            var cts = new CancellationTokenSource();
            try
            {
                await k8s.WatchAsync<V1Pod>(
                    async (@event) =>
                    {
                        await SyncContext.Clear;

                        clusterPods[@event.Value.Uid()] = @event.Value;

                        if (@event.Value.Status.ContainerStatuses?.All(cs => cs.Ready == true) == true)
                        {
                            try
                            {
                                if (!cluster.Status.Cluster.Nodes.ContainsKey(@event.Value.Uid()))
                                {
                                    cluster.Status.Cluster.Nodes.Add(@event.Value.Uid(), new GarnetNode()
                                    {
                                        Address   = cluster.CreatePodAddress(@event.Value),
                                        Namespace = cluster.Namespace(),
                                        NodeName  = @event.Value.Spec.NodeName,
                                        PodName   = @event.Value.Name(),
                                        PodUid    = @event.Value.Uid(),
                                        Port      = Constants.Ports.Redis,
                                        Role      = GarnetRole.None,
                                        Zone      = @event.Value.Spec.NodeName
                                    });
                                }
                                else
                                {
                                    cluster.Status.Cluster.Nodes[@event.Value.Uid()].Address = cluster.CreatePodAddress(@event.Value);
                                }

                                if (clusterPods.Values.All(p => p.Status.ContainerStatuses?.All(cs => cs.Ready == true) == true))
                                {
                                    await cts.CancelAsync();
                                }
                            }
                            catch (Exception e)
                            {
                                logger?.LogErrorEx(e);
                            }
                        }

                    },
                    namespaceParameter: cluster.Metadata.NamespaceProperty,
                    labelSelector: $"{Constants.Labels.ManagedByKey}={Constants.OperatorName},{Constants.Labels.ClusterId}={cluster.Uid()}",
                    retryDelay: TimeSpan.FromSeconds(30),
                    logger: logger,
                    cancellationToken: cts.Token);
            }
            catch (TaskCanceledException e)
            {
                logger?.LogErrorEx(e);
            }
            catch (OperationCanceledException e)
            {
                logger?.LogErrorEx(e);
            }

            await SaveStatusAsync(cancellationToken);
        }
        
        internal V1PodSpec CreatePodSpec()
        {
            var image = cluster.Spec.Image ?? new ImageSpec();

            return new V1PodSpec()
            {
                Containers =
                [
                    new V1Container()
                    {
                        Name            = Constants.GarnetContainer,
                        SecurityContext = cluster.Spec.SecurityContext,
                        Image           = image.ToString(),
                        ImagePullPolicy = image.PullPolicy,
                        Command =
                        [
                            "/app/GarnetServer"
                        ],
                        Args =
                        [
                            "--cluster",
                            "--aof",
                            "--bind", "$(POD_IP)",
                            "--port", Constants.Ports.Redis.ToString(),
                            "-i", "64m"
                        ],
                        Ports = 
                        [
                            new V1ContainerPort(){
                                Name = Constants.Ports.RedisName,
                                ContainerPort = Constants.Ports.Redis,
                                Protocol = "TCP"
                            }
                        ],
                        Env =
                        [
                            new V1EnvVar()
                            {
                                Name = "POD_IP",
                                ValueFrom = new V1EnvVarSource()
                                {
                                    FieldRef = new V1ObjectFieldSelector()
                                    {
                                        FieldPath = "status.podIP"
                                    }
                                }
                            }
                        ],
                        Resources = cluster.Spec.Resources,
                    }
                ],
                NodeSelector = cluster.Spec.NodeSelector,
                Affinity     = cluster.Spec.Affinity,
                Tolerations  = cluster.Spec.Tolerations
            };
        }

        internal bool NeedsMorePods()
        {
            var numPodsRequired = NumPodsRequired();

            if (cluster.Status == null)
            {
                return true;
            }

            //if (cluster.Status.Cluster.NumberOfPods != cluster.Status.Cluster.NumberOfPodsReady)
            //{
            //    return false;
            //}

            if (clusterPods.Count() < numPodsRequired)
            {
                return true;
            }

            return false;
        }

        internal bool NeedsLessPods()
        {
            var numPodsRequired = NumPodsRequired();

            if (cluster.Status == null)
            {
                return false;
            }

            //if (cluster.Status.Cluster.NumberOfPods != cluster.Status.Cluster.NumberOfPodsReady)
            //{
            //    return false;
            //}

            if (clusterPods.Count() > numPodsRequired)
            {
                return true;
            }

            return false;
        }

        internal int NumPodsRequired()
        {
            return cluster.Spec.NumberOfPrimaries * (1 + cluster.Spec.ReplicationFactor);
        }

        internal async Task<Dictionary<string, V1Pod>> GetClusterPodsAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            var pods = await k8s.CoreV1.ListNamespacedPodAsync(
                namespaceParameter: cluster.Metadata.NamespaceProperty,
                labelSelector:      cluster.Spec.AdditionalLabels.ToLabelSelector(),
                cancellationToken:  cancellationToken);

            var result = new Dictionary<string, V1Pod>();
            foreach (var pod in pods)
            {
                result[pod.Uid()] = pod;
            }

            var podIds = pods.Items.Select(p => p.Uid());

            foreach (var node in cluster.Status.Cluster.Nodes)
            {
                if (!podIds.Contains(node.Key))
                {
                    cluster.Status.Cluster.Nodes.Remove(node.Key);

                    await SaveStatusAsync(cancellationToken);
                }
            }

            return result;
        }

        internal async Task ReconcileConfigMapAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            logger?.LogInformationEx(() => $"Reconciling ConfigMap for {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");
            logger?.LogInformationEx(() => $"Reconciled ConfigMap for {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");
        }

        internal async Task ReconcileServicesAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            logger?.LogInformationEx(() => $"Reconciling Services for {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");

            await ReconcileServiceAsync(
                serviceName:       cluster.Spec.ServiceName ?? cluster.Name(),
                headless:          false,
                cancellationToken: cancellationToken);

            await ReconcileServiceAsync(
                serviceName:       (cluster.Spec.ServiceName ?? cluster.Name()) + "-headless",
                headless:          true,
                cancellationToken: cancellationToken);

            logger?.LogInformationEx(() => $"Reconciled Services for {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");
        }

        internal async Task ReconcileServiceAsync(string serviceName, bool headless, CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            var existingService = new V1Service();

            var exists = false;
            try
            {
                existingService = await k8s.CoreV1.ReadNamespacedServiceAsync(
                    name:               serviceName,
                    namespaceParameter: cluster.Metadata.NamespaceProperty,
                    cancellationToken:  cancellationToken);

                exists          = true;
            }
            catch (HttpOperationException e) when (e.Response.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                existingService                            = new V1Service().Initialize();
                existingService.Metadata.Name              = serviceName;
                existingService.Metadata.NamespaceProperty = cluster.Metadata.NamespaceProperty;
            }

            bool hasChanges = false;

            var service = KubernetesHelper.JsonClone(existingService);

            service.EnsureMetadata();
            service.Metadata.EnsureLabels();
            service.SetLabel(Constants.Labels.ManagedByKey, Constants.OperatorName);
            service.SetLabel(Constants.Labels.ClusterId, cluster.Uid());

            service.Spec          ??= new V1ServiceSpec();
            service.Spec.Selector ??= new Dictionary<string, string>();

            service.Spec.Type                                    = "ClusterIP";
            service.Spec.InternalTrafficPolicy                   = "Cluster";
            service.Spec.IpFamilyPolicy                          = "SingleStack";
            service.Spec.SessionAffinity                         = "None";
            service.Spec.IpFamilies                              = ["IPv4"];
            service.Spec.Selector[Constants.Labels.ManagedByKey] = Constants.OperatorName;
            service.Spec.Selector[Constants.Labels.ClusterId]    = cluster.Uid();
            service.Spec.Ports                                   =
            [
                new V1ServicePort()
                {
                    Name          = Constants.Ports.RedisName,
                    Protocol      = "TCP",
                    Port          = Constants.Ports.Redis,
                    TargetPort    = Constants.Ports.Redis,
                    AppProtocol   = "tcp",
                },
            ];

            service.Spec.Selector.AddRange(cluster.Spec.AdditionalLabels);

            if (headless)
            {
                service.Spec.ClusterIP = "None";
            }

            hasChanges = !service.JsonEquals(existingService);
            hasChanges = service.Metadata.SetOwnerReference(cluster.MakeOwnerReference()) || hasChanges;

            if (!exists)
            {
                await k8s.CoreV1.CreateNamespacedServiceAsync(
                    body:               service,
                    namespaceParameter: cluster.Metadata.NamespaceProperty,
                    cancellationToken:  cancellationToken);
                return;
            }

            if (hasChanges)
            {
                await k8s.CoreV1.ReplaceNamespacedServiceAsync(
                    body:               service,
                    name:               serviceName,
                    namespaceParameter: cluster.Metadata.NamespaceProperty,
                    cancellationToken:  cancellationToken);

                return;
            }
        }

        
    }
}
