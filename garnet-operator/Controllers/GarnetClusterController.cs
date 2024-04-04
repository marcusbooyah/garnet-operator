using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Policy;
using System.Threading;
using System.Threading.Tasks;

using Garnet.client;
using Garnet.cluster;

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
using Neon.Operator.Core;
using Neon.Operator.Core.Exceptions;
using Neon.Operator.Attributes;
using Neon.Operator.Controllers;
using Neon.Operator.Finalizers;
using Neon.Operator.Rbac;
using Neon.Tasks;
using Neon.Operator.Util;

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
    [ResourceController(AutoRegisterFinalizers = true, ErrorMaxRequeueIntervalSeconds = 60, MaxConcurrentReconciles = 10, MaxConcurrentFinalizers = 10)]
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
        private IEnumerable<GarnetNode> demoting => cluster.Status.Cluster.Nodes.Values.Where(n => n.Role == GarnetRole.Demoting);
        private IEnumerable<GarnetNode> orphans => cluster.Status.Cluster.Nodes.Values.Where(n => n.Role == GarnetRole.None);
        private IEnumerable<GarnetNode> allNodes => cluster.Status.Cluster.Nodes.Values;
        private IEnumerable<ClusterNode> clusterNodes;
        private string headlessServiceName => (cluster.Spec.ServiceName ?? cluster.Name()) + "-headless";
        private int numReplicas => cluster.Spec.ReplicationFactor* cluster.Spec.NumberOfPrimaries;

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

        public override Task<ErrorPolicyResult> ErrorPolicyAsync(V1alpha1GarnetCluster entity, int attempt, Exception exception, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new ErrorPolicyResult(TimeSpan.Zero, Neon.Operator.Controllers.WatchEventType.Modified, true));
        }

        /// <inheritdoc/>
        public override async Task<ResourceControllerResult> ReconcileAsync(V1alpha1GarnetCluster resource, CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();
            
            cluster = resource;

            logger?.LogInformationEx(() => $"Reconciling: {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });

            await InitializeStatusAsync(cancellationToken);
            await GetClusterPodsAsync(cancellationToken);

            var tasks = new List<Task>()
            {
                ReconcileConfigMapAsync(cancellationToken),
                ReconcileServicesAsync(cancellationToken)
            };

            await Task.WhenAll(tasks);

            await GetCurrentStatusAsync();

            await ReconcileClusterAsync(cancellationToken);

            await InitializeClusterAsync(cancellationToken);

            await ConfigureClusterAsync(cancellationToken);

            //await GetShardsAsync(cancellationToken);

            if (primaries.Count() != cluster.Spec.NumberOfPrimaries
                || replicas.Count() != numReplicas)
            {
                throw new RequeueException("", TimeSpan.Zero, Neon.Operator.Controllers.WatchEventType.Modified);
            }

            await cluster.SetConditionAsync(
                k8s:               k8s,
                type:              Constants.Conditions.Scaling,
                status:            Constants.Conditions.StatusFalse,
                reason:            string.Empty,
                message:           string.Empty,
                cancellationToken: cancellationToken);

            await RebalancePrimariesAsync(cancellationToken);
            await RebalanceReplicasAsync(cancellationToken);
            await CleanupAsync(cancellationToken);

            logger?.LogInformationEx(() => $"Reconciled: {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });

            await cluster.SetConditionAsync(
                k8s:               k8s,
                type:              Constants.Conditions.ClusterOk,
                status:            Constants.Conditions.StatusTrue,
                reason:            string.Empty,
                message:           string.Empty,
                cancellationToken: cancellationToken);

            return ResourceControllerResult.Ok();
        }
        internal GarnetNode ClusterIdToGarnetNode(string id) => allNodes.Where(n => n.Id == id).FirstOrDefault();
        internal GarnetNode PodIdToGarnetNode(string id) => allNodes.Where(n => n.PodUid == id).FirstOrDefault();

        internal async Task CleanupAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            logger?.LogInformationEx(() => $"Cleaning up cluster", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });

            var client = await garnetHelper.CreateClientAsync(primaries.SelectRandom().First());

            var nodes = await client.GetNodesAsync();

            foreach (var node in nodes)
            {
                if (allNodes.Any(n => n.Id == node.Id))
                {
                    continue;
                }

                await garnetHelper.ForgetNodeAsync(node.Id, allNodes, cancellationToken);
            }
        }

        internal async Task GetNodesAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            var client = await garnetHelper.CreateClientAsync(primaries.SelectRandom().First());

            clusterNodes = await client.GetNodesAsync(cancellationToken);

            foreach (var node in clusterNodes)
            {
                var rn = allNodes.Where(cn => cn.Id == node.Id).FirstOrDefault();
                if (rn != null)
                {
                    rn.PodIp = node.IpAddress;
                }
            }

            await SaveStatusAsync(cancellationToken);
        }

        internal async Task InitializeStatusAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            logger?.LogInformationEx(() => $"Initializing cluster status.", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });

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
                StartTime  = DateTime.UtcNow,
            };

            var meta = typeof(V1alpha1GarnetCluster).GetKubernetesTypeMetadata();

            cluster = await k8s.CustomObjects.ReplaceNamespacedCustomObjectStatusAsync(
                @object:            cluster,
                namespaceParameter: cluster.Namespace(),
                cancellationToken:  cancellationToken);
        }

        internal async Task ReconcileClusterAsync(CancellationToken cancellationToken = default)
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
                await GetNodesAsync(cancellationToken);
                await RebalancePrimariesAsync(cancellationToken);
                await ScaleDownAsync(cancellationToken);
                await WaitForPodReadinessAsync(cancellationToken);
            }
        }

        internal async Task RebalancePrimariesAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            logger?.LogInformationEx(() => $"Rebalancing primaries", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });

            var numSlices    = cluster.Spec.NumberOfPrimaries;
            var slotsPerNode = Constants.Redis.Slots / numSlices;

            var newNodes = primaries.Where(p => p.Slots != null && p.Slots.Count > 0).OrderBy(p => p.Slots[0]).ToList();
            newNodes.AddRange(primaries.Where(p => p.Slots == null || p.Slots.Count == 0));
            newNodes = KubernetesHelper.JsonClone(newNodes.Take(numSlices).ToList());

            for (int i = 0; i < numSlices; i++)
            {
                var start = (i) * (Constants.Redis.Slots / cluster.Spec.NumberOfPrimaries);
                var end   = (1 + i) * (Constants.Redis.Slots / cluster.Spec.NumberOfPrimaries) - 1;

                if (i == cluster.Spec.NumberOfPrimaries - 1)
                {
                    end = Constants.Redis.Slots - 1;
                }

                newNodes[i].Slots = new List<int> { start, end };
            }

            var slotsToMigrate = new Dictionary<string, Dictionary<string, SlotMigration>>();
            var slotsToCreate = new Dictionary<GarnetNode, List<int>>();
            foreach (var node in primaries)
            {
                var newNode = newNodes.Where(n => n.Id == node.Id).FirstOrDefault();

                foreach (var slot in node.GetSlots())
                {
                    if (newNode != null)
                    {
                        if (newNode.Slots.Count > 0
                            && (slot >= newNode.Slots.First() && slot <= newNode.Slots.Last()))
                        {
                            continue;
                        }
                    }

                    var toNode = newNodes.Where(n => slot >= n.Slots.First() && slot <= n.Slots.Last()).FirstOrDefault();

                    if (toNode == null)
                    {
                        continue;
                    }

                    if (!slotsToMigrate.ContainsKey(node.Id))
                    {
                        slotsToMigrate[node.Id] = new Dictionary<string, SlotMigration>();
                    }


                    if (!slotsToMigrate[node.Id].ContainsKey(toNode.Id))
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
                var node   = ClusterIdToGarnetNode(batch.Key);
                var client = await garnetHelper.CreateClientAsync(node);

                foreach (var toNode in batch.Value)
                {
                    foreach (var r in toNode.Value.GetSlotRanges())
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var toClient = await garnetHelper.CreateClientAsync(toNode.Value.ToNode);
                        var info     = await toClient.GetSelfAsync();

                        if (!info.Slots.Contains(r.Min) && !info.Slots.Contains(r.Max))
                        {
                            logger?.LogInformationEx(() => $"Moving slots [{r.Min} {r.Max}] to [{toNode.Value.ToNode.Address}]", attributes =>
                            {
                                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                            });

                            await client.MigrateSlotsRangeAsync(
                                address: toNode.Value.ToNode.PodIp,
                                port: toNode.Value.ToNode.Port,
                                start: r.Min,
                                end: r.Max);

                            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken: cancellationToken);
                        }

                        info = await toClient.GetSelfAsync();

                        cluster.Status.Cluster.Nodes[toNode.Value.ToNode.PodUid].Slots = info.Slots;

                        var firstPrimaryInfo = await client.GetSelfAsync();

                        cluster.Status.Cluster.Nodes[node.PodUid].Slots = firstPrimaryInfo.Slots;

                        await SaveStatusAsync(cancellationToken);
                    }
                }
                
            }
        }

        internal async Task RebalanceReplicasAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            logger?.LogInformationEx(() => $"Rebalancing replicas.", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });

            foreach (var shard in cluster.Status.Cluster.NumberOfReplicasPerPrimary.Where(p => p.Value > cluster.Spec.ReplicationFactor).Select(p => p.Key))
            {
                while (cluster.Status.Cluster.NumberOfReplicasPerPrimary[shard] > cluster.Spec.ReplicationFactor)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var shardPrimary    = PodIdToGarnetNode(shard);
                    var replicaToMove   = replicas.Where(r => r.PrimaryId == shardPrimary.Id).SelectRandom(1).First();
                    var node            = ClusterIdToGarnetNode(replicaToMove.Id);
                    var existingPrimary = ClusterIdToGarnetNode(node.PrimaryId);

                    if (node.Role == GarnetRole.Demoting)
                    {
                        continue;
                    }

                    var client = await garnetHelper.CreateClientAsync(node);

                    var primaryCandidates = cluster
                        .Status
                        .Cluster
                        .NumberOfReplicasPerPrimary
                        .Where(x => x.Value < cluster.Spec.ReplicationFactor)
                        .OrderBy(x => x.Value);

                    string primaryUid = null;

                    if (primaryCandidates.Any(p => PodIdToGarnetNode(p.Key).NodeName != node.NodeName))
                    {
                        primaryUid = primaryCandidates.Where(p => PodIdToGarnetNode(p.Key).NodeName != node.NodeName).First().Key;
                    }
                    else
                    {
                        primaryUid = primaryCandidates.First().Key;
                    }

                    var primary = PodIdToGarnetNode(primaryUid);

                    logger?.LogInformationEx(() => $"Moving replica [{replicaToMove.Address}] to [{primary.Address}]");

                    var status = await client.GetSelfAsync(cancellationToken);

                    if (!status.MasterId.IsNullOrEmpty())
                    {
                        await client.DetachReplicaAsync(cancellationToken);
                    }

                    await client.ReplicateAsync(primary.Id);

                    node.Role      = GarnetRole.Replica;
                    node.PrimaryId = primary.Id;

                    cluster.Status.Cluster.NumberOfReplicasPerPrimary[existingPrimary.PodUid]--;

                    await SaveStatusAsync(cancellationToken);
                }
            }
        }

        internal async Task InitializeClusterAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            logger?.LogInformationEx(() => $"Initializing cluster.", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });

            if (cluster.Status.Conditions.Any(c => c.Type == Constants.Conditions.Initialized
                && c.Status == Constants.Conditions.StatusTrue))
            {
                return;
            }

            await cluster.SetConditionAsync(
                k8s:               k8s,
                type:              Constants.Conditions.Initialized,
                status:            Constants.Conditions.StatusFalse,
                reason:            "Cluster is being initialized",
                message:           "Cluster is being initialized",
                cancellationToken: cancellationToken);

            GarnetClient clusterClient = null;
            if (primaries.IsEmpty())
            {
                var pod = clusterPods.First().Value;

                logger?.LogInformationEx(() => $"Pod {pod.Name()}/{pod.Namespace()} is first master.", attributes =>
                {
                    attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                });

                clusterClient = await garnetHelper.CreateClientAsync(pod, Constants.Ports.Redis);

                await clusterClient.ClusterResetAsync(true);

                var id = await clusterClient.MyIdAsync();

                var slotStart = 0;
                var slotEnd   = Constants.Redis.Slots / cluster.Spec.NumberOfPrimaries - 1;

                cluster.Status.Cluster.Nodes[pod.Uid()].Id                   = id;
                cluster.Status.Cluster.Nodes[pod.Uid()].Role                 = GarnetRole.Primary;
                cluster.Status.Cluster.Nodes[pod.Uid()].Slots                = [slotStart, slotEnd];
                cluster.Status.Cluster.Nodes[pod.Uid()].PodUid               = pod.Uid();
                cluster.Status.Cluster.Nodes[pod.Uid()].PodName              = pod.Metadata.Name;
                cluster.Status.Cluster.Nodes[pod.Uid()].Namespace            = cluster.Metadata.NamespaceProperty;
                cluster.Status.Cluster.Nodes[pod.Uid()].Port                 = Constants.Ports.Redis;
                cluster.Status.Cluster.Nodes[pod.Uid()].Address              = cluster.CreatePodAddress(pod);
                cluster.Status.Cluster.Nodes[pod.Uid()].PodIp                = pod.Status.PodIP;
                cluster.Status.Cluster.Nodes[pod.Uid()].ConfigEpoch          = 1;
                cluster.Status.Cluster.Nodes[pod.Uid()].NodeName             = pod.Spec.NodeName;
                cluster.Status.Cluster.Nodes[pod.Uid()].Zone                 = pod.Spec.NodeName;
                cluster.Status.Cluster.NumberOfReplicasPerPrimary[pod.Uid()] = 0;
                cluster.Status.LastEpoch                                     = 1;

                await clusterClient.AddSlotsRangeAsync(
                    start: slotStart,
                    end:   slotEnd);

                await clusterClient.SetConfigEpochAsync(1);

                await SaveStatusAsync(cancellationToken);
            }

            var initPrimary   = primaries.Where(n => n.ConfigEpoch == 1).FirstOrDefault();

            clusterClient ??= await garnetHelper.CreateClientAsync(initPrimary);

            while (primaries.Count() < cluster.Spec.NumberOfPrimaries)
            {
                var pod    = orphans.First();

                logger?.LogInformationEx(() => $"Adding pod {pod.PodName}/{pod.Namespace} as master", attributes =>
                {
                    attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                });

                var k8sPod = clusterPods[pod.PodUid];
                var client = await garnetHelper.CreateClientAsync(pod);
                var node   = primaries.Count();

                var start = (node) * (Constants.Redis.Slots / cluster.Spec.NumberOfPrimaries);
                var end   = (1 + node) * (Constants.Redis.Slots / cluster.Spec.NumberOfPrimaries) - 1;

                if (node == cluster.Spec.NumberOfPrimaries - 1)
                {
                    end = Constants.Redis.Slots - 1;
                }

                await client.ClusterResetAsync(true);

                var clusterInfo = await clusterClient.ClusterInfoAsync();
                var epoch       = clusterInfo.CurrentEpoch + 1;
                var id          = await client.MyIdAsync();

                cluster.Status.Cluster.Nodes[pod.PodUid].Id                   = id;
                cluster.Status.Cluster.Nodes[pod.PodUid].Role                 = GarnetRole.Primary;
                cluster.Status.Cluster.Nodes[pod.PodUid].Slots                = [start, end];
                cluster.Status.Cluster.Nodes[pod.PodUid].ConfigEpoch          = epoch;
                cluster.Status.Cluster.NumberOfReplicasPerPrimary[pod.PodUid] = 0;
                cluster.Status.LastEpoch                                      = epoch;

                await client.AddSlotsRangeAsync(
                    start: start,
                    end:   end);

                await client.SetConfigEpochAsync(epoch);

                await clusterClient.MeetAsync(pod.Address, pod.Port);

                await SaveStatusAsync(cancellationToken);
            }

            await cluster.SetConditionAsync(
                k8s:               k8s,
                type:              Constants.Conditions.Initialized,
                status:            Constants.Conditions.StatusTrue,
                reason:            "Cluster is being initialized",
                message:           "Cluster is being initialized",
                cancellationToken: cancellationToken);

            await SaveStatusAsync(cancellationToken);
        }

        internal async Task ConfigureClusterAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            GarnetClient clusterClient = null;

            clusterClient ??= await garnetHelper.CreateClientAsync(primaries.First());

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

                    var replicaClient = await garnetHelper.CreateClientAsync(pod);
                    var clusterInfo   = await clusterClient.ClusterInfoAsync();
                    var epoch         = clusterInfo.CurrentEpoch + 1;

                    await replicaClient.ClusterResetAsync(true);
                    await replicaClient.SetConfigEpochAsync(epoch);

                    var id = await replicaClient.MyIdAsync();

                    cluster.Status.Cluster.Nodes[pod.PodUid].Id          = id;
                    cluster.Status.Cluster.Nodes[pod.PodUid].ConfigEpoch = epoch;

                    var resp = await clusterClient.MeetAsync(pod.Address);
                }

                if (pod == null)
                {
                    return;
                }

                logger?.LogInformationEx(() => $"Adding pod {pod.PodName}/{pod.Namespace} as master", attributes =>
                {
                    attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                });

                cluster.Status.Cluster.Nodes[pod.PodUid].Role                 = GarnetRole.Primary;
                cluster.Status.Cluster.NumberOfReplicasPerPrimary[pod.PodUid] = 0;

                await SaveStatusAsync(cancellationToken);
            }

            while (replicas.Count() < numReplicas)
            {
                GarnetNode primary = null;
                GarnetNode pod = null;
                if (orphans.Any())
                {
                    pod  = orphans.First();
                }

                if (pod == null)
                {
                    pod = primaries.OrderBy(p => p.NumSlots()).FirstOrDefault();
                }

                logger?.LogInformationEx(() => $"Adding pod {pod.PodName}/{pod.Namespace} as replica", attributes =>
                {
                    attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                });

                var primaryUid = cluster
                    .Status
                    .Cluster
                    .NumberOfReplicasPerPrimary
                    .Where(p => p.Key != pod.PodUid)
                    .Where(p => primaries.Any(pr => pr.PodUid == p.Key))
                    .OrderBy(p => p.Value).FirstOrDefault().Key;

                if (!cluster.Status.Cluster.NumberOfReplicasPerPrimary.TryGetValue(primaryUid, out var primaryReplicas))
                {
                    primaryReplicas = 0;
                };

                primary   = cluster.Status.Cluster.Nodes[primaryUid];


                var replicaClient = await garnetHelper.CreateClientAsync(pod);
                var primaryClient = await garnetHelper.CreateClientAsync(primary);
                var clusterInfo   = await primaryClient.ClusterInfoAsync(cancellationToken);
                var epoch         = clusterInfo.CurrentEpoch + 1;

                await garnetHelper.ForgetNodeAsync(pod, allNodes);

                await replicaClient.ClusterResetAsync(true);

                await replicaClient.SetConfigEpochAsync(epoch);

                await replicaClient.MeetAsync(primary.Address, primary.Port);
                await replicaClient.ReplicateAsync(primary.Id);

                var replicaId     = await replicaClient.MyIdAsync();

                cluster.Status.Cluster.NumberOfReplicasPerPrimary[primaryUid] = primaryReplicas + 1;
                cluster.Status.Cluster.Nodes[pod.PodUid].Id                   = replicaId;
                cluster.Status.Cluster.Nodes[pod.PodUid].Role                 = GarnetRole.Replica;
                cluster.Status.Cluster.Nodes[pod.PodUid].PrimaryId            = primary.Id;
                cluster.Status.LastEpoch                                      = epoch;

                await SaveStatusAsync(cancellationToken);
                
            }
        }

        internal async Task SaveStatusAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            logger?.LogInformationEx(() => $"Saving status", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });

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

            logger?.LogInformationEx(() => $"Scaling down", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });

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
                cluster.Status.Cluster.NumberOfReplicasPerPrimary.Remove(pod.PodUid);

                logger?.LogInformationEx(() => $"Marking master pod {pod.PodName}/{pod.Namespace} for removal", attributes =>
                {
                    attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                });

                await SaveStatusAsync(cancellationToken);
            }

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
                    logger?.LogInformationEx(() => $"Marking replica pod {pod.PodName}/{pod.Namespace} for removal", attributes =>
                    {
                        attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                    });

                    if (cluster.Status.Cluster.Nodes[pod.PodUid].Role == GarnetRole.Replica)
                    {
                        var primary = ClusterIdToGarnetNode(pod.PrimaryId);
                        cluster.Status.Cluster.NumberOfReplicasPerPrimary[primary.PodUid]--;
                    }

                    cluster.Status.Cluster.Nodes[pod.PodUid].Role = GarnetRole.Leaving;
                }
                else
                {
                    logger?.LogInformationEx(() => $"Marking replica pod {pod.PodName}/{pod.Namespace} for promotion", attributes =>
                    {
                        attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                    });

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

            logger?.LogInformationEx(() => $"Removing pods marked for removal.", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });

            foreach (var leavingNode in leaving)
            {
                await garnetHelper.ForgetNodeAsync(leavingNode, allNodes);

                try
                {
                    await k8s.CoreV1.DeleteNamespacedPodAsync(
                        name:               leavingNode.PodName,
                        namespaceParameter: leavingNode.Namespace,
                        cancellationToken:  cancellationToken);
                }
                catch (HttpOperationException e) when (e.Response.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    logger?.LogInformationEx(() => "Pod has alraedy been deleted", attributes =>
                    {
                        attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                    });
                }
                catch (Exception e)
                {
                    logger?.LogErrorEx(e, () => string.Empty, attributes =>
                    {
                        attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                    });
                    continue;
                }

                cluster.Status.Cluster.TryRemoveNode(leavingNode.PodUid);

                await SaveStatusAsync(cancellationToken);
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

                var podName = cluster.CreatePodName();
                pod.Metadata.EnsureLabels();
                pod.Metadata.EnsureAnnotations();
                    
                pod.Metadata.Name              = podName;
                pod.Metadata.NamespaceProperty = cluster.Metadata.NamespaceProperty;

                pod.Metadata.Labels.AddRange(cluster.Spec.AdditionalLabels);
                pod.Metadata.SetOwnerReference(cluster.MakeOwnerReference());
                pod.Metadata.Labels.Add(Constants.Labels.ManagedByKey, Constants.OperatorName);
                pod.Metadata.Labels.Add(Constants.Labels.ClusterId, cluster.Uid());
                pod.Metadata.Labels.Add(Constants.Labels.ClusterName, cluster.Metadata.Name);
                pod.Metadata.Labels.Add(Constants.Labels.PodName, podName);

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

        internal async Task GetCurrentStatusAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            foreach (var pod in clusterPods)
            {
                var client = await garnetHelper.CreateClientAsync(pod.Value, Constants.Ports.Redis);

                var self = await client.GetSelfAsync();

                cluster.Status.Cluster.Nodes.TryGetValue(pod.Value.Uid(), out var status);

                status ??= new GarnetNode();

                status.PodIp     = pod.Value.Status.PodIP;

                await SaveStatusAsync(cancellationToken);
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

            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
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
                                        Zone      = @event.Value.Spec.NodeName,
                                        PodIp     = @event.Value.Status.PodIP
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
                                logger?.LogErrorEx(e, () => string.Empty, attributes =>
                                {
                                    attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                                });
                            }
                        }

                    },
                    namespaceParameter: cluster.Metadata.NamespaceProperty,
                    labelSelector:      $"{Constants.Labels.ManagedByKey}={Constants.OperatorName},{Constants.Labels.ClusterId}={cluster.Uid()}",
                    retryDelay:         TimeSpan.FromSeconds(30),
                    logger:             logger,
                    cancellationToken:  cts.Token);
            }
            catch (TaskCanceledException e)
            {
                logger?.LogErrorEx(e, () => string.Empty, attributes =>
                {
                    attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                });
            }
            catch (OperationCanceledException e)
            {
                logger?.LogErrorEx(e, () => string.Empty, attributes =>
                {
                    attributes.Add(Constants.Labels.ClusterName, cluster.Name());
                });
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
                        LivenessProbe = new V1Probe(){
                            Exec = new V1ExecAction()
                            {
                                Command =
                                [
                                    "bash",
                                    "-c",
                                    "redis-cli -h $POD_IP ping"
                                ]
                            },
                            InitialDelaySeconds = 10,
                            TimeoutSeconds      = 5,
                            PeriodSeconds       = 5,
                            FailureThreshold    = 30,
                            SuccessThreshold    = 1,
                        },
                        ReadinessProbe = new V1Probe(){
                            Exec = new V1ExecAction()
                            {
                                Command =
                                [
                                    "bash",
                                    "-c",
                                    "redis-cli -h $POD_IP ping"
                                ]
                            },
                            InitialDelaySeconds = 10,
                            TimeoutSeconds      = 5,
                            PeriodSeconds       = 5,
                            FailureThreshold    = 30,
                            SuccessThreshold    = 1,
                        },
                        StartupProbe = new V1Probe(){
                            Exec = new V1ExecAction()
                            {
                                Command =
                                [
                                    "bash",
                                    "-c",
                                    "redis-cli -h $POD_IP ping"
                                ]
                            },
                            InitialDelaySeconds = 10,
                            TimeoutSeconds      = 5,
                            PeriodSeconds       = 5,
                            FailureThreshold    = 30,
                            SuccessThreshold    = 1,
                        }
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

        internal async Task GetClusterPodsAsync(CancellationToken cancellationToken = default)
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
                //if (cluster.Status.Cluster.Nodes.TryGetValue(pod.Uid(), out var status))
                //{
                //    status.PodName   = pod.Metadata.Name;
                //    status.Namespace = cluster.Metadata.NamespaceProperty;
                //    status.Port      = Constants.Ports.Redis;
                //    status.Address   = cluster.CreatePodAddress(pod);
                //    status.PodIp     = pod.Status.PodIP;
                //    status.NodeName  = pod.Spec.NodeName;
                //    status.Zone      = pod.Spec.NodeName;
                //}
            }

            //await SaveStatusAsync(cancellationToken);

            var podIds = pods.Items.Select(p => p.Uid());

            foreach (var node in cluster.Status.Cluster.Nodes)
            {
                if (!podIds.Contains(node.Key))
                {
                    cluster.Status.Cluster.Nodes.Remove(node.Key);

                    await SaveStatusAsync(cancellationToken);
                }
            }

            clusterPods = result;
        }

        internal async Task ReconcileConfigMapAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            logger?.LogInformationEx(() => $"Reconciling ConfigMap", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });
            logger?.LogInformationEx(() => $"Reconciled ConfigMap", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });
        }

        internal async Task ReconcileServicesAsync(CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            logger?.LogInformationEx(() => $"Reconciling Services", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });

            await ReconcileServiceAsync(
                serviceName:       cluster.Spec.ServiceName ?? cluster.Name(),
                headless:          false,
                cancellationToken: cancellationToken);

            await ReconcileServiceAsync(
                serviceName:       (cluster.Spec.ServiceName ?? cluster.Name()) + "-headless",
                headless:          true,
                cancellationToken: cancellationToken);

            logger?.LogInformationEx(() => $"Reconciled Services", attributes =>
            {
                attributes.Add(Constants.Labels.ClusterName, cluster.Name());
            });
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
