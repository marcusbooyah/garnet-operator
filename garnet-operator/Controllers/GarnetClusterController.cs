using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Garnet.client;

using GarnetOperator.Models;

using k8s;
using k8s.Autorest;
using k8s.Models;

using Microsoft.Extensions.Logging;

using Neon.Common;
using Neon.Diagnostics;
using Neon.Operator;
using Neon.Operator.Attributes;
using Neon.Operator.Controllers;
using Neon.Operator.Finalizers;
using Neon.Operator.Rbac;
using Neon.Tasks;

namespace GarnetOperator
{
    /// <summary>
    /// Controller for managing GarnetCluster resources.
    /// </summary>
    [RbacRule<V1Pod>(Scope = EntityScope.Cluster, Verbs = RbacVerb.All)]
    [RbacRule<V1Service>(Scope = EntityScope.Cluster, Verbs = RbacVerb.All)]
    [RbacRule<V1ConfigMap>(Scope = EntityScope.Cluster, Verbs = RbacVerb.All)]
    [RbacRule<V1PodDisruptionBudget>(Scope = EntityScope.Cluster, Verbs = RbacVerb.All)]
    [RbacRule<V1alpha1GarnetCluster>(Scope = EntityScope.Cluster, Verbs = RbacVerb.All)]
    public class GarnetClusterController : ResourceControllerBase<V1alpha1GarnetCluster>
    {
        private readonly IKubernetes                              k8s;
        private readonly IFinalizerManager<V1alpha1GarnetCluster> finalizerManager;
        private readonly ILogger<GarnetClusterController>         logger;
        private readonly GarnetClient                             garnetClient;

        private static TimeSpan RequeueDelay = TimeSpan.FromSeconds(20);

        private V1alpha1GarnetCluster cluster;

        /// <summary>
        /// Initializes a new instance of the <see cref="GarnetClusterController"/> class.
        /// </summary>
        /// <param name="k8s">The Kubernetes client.</param>
        /// <param name="finalizerManager">The finalizer manager.</param>
        /// <param name="logger">The logger.</param>
        public GarnetClusterController(
            IKubernetes                              k8s,
            GarnetClient                             garnetClient,
            IFinalizerManager<V1alpha1GarnetCluster> finalizerManager,
            ILogger<GarnetClusterController>         logger)
        {
            this.k8s              = k8s;
            this.garnetClient     = garnetClient;
            this.finalizerManager = finalizerManager;
            this.logger           = logger;
        }

        /// <inheritdoc/>
        public override async Task<ResourceControllerResult> ReconcileAsync(V1alpha1GarnetCluster resource)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();
            
            cluster = resource;

            logger.LogInformationEx(() => $"Reconciling: {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");


            var tasks = new List<Task>()
            {
                ReconcileConfigMapAsync(),
                ReconcileServicesAsync()
            };

            await Task.WhenAll(tasks);

            

            logger.LogInformationEx(() => $"Reconciled: {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");

            return ResourceControllerResult.Ok();
        }


        internal async Task<ResourceControllerResult> ReconcileClusterAsync()
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            var needsMorePods = NeedsMorePods();
            var needsLessPods = NeedsLessPods();

            if (needsMorePods)
            {
                await ScaleUpAsync();

                return ResourceControllerResult.RequeueEvent(RequeueDelay);
            }
            else if (needsLessPods)
            {
                await ScaleDownAsync();

                return ResourceControllerResult.RequeueEvent(RequeueDelay);
            }
            else
            {
                await cluster.SetConditionAsync(
                   k8s:     k8s,
                   type:    Constants.Conditions.Scaling,
                   status:  Constants.Conditions.StatusFalse,
                   reason:  string.Empty,
                   message: string.Empty);
            }

            return ResourceControllerResult.Ok();
        }

        internal async Task ScaleDownAsync()
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            await cluster.SetConditionAsync(
                k8s:     k8s,
                type:    Constants.Conditions.Scaling,
                status:  Constants.Conditions.StatusTrue,
                reason:  Constants.Conditions.ScalingDownReason,
                message: Constants.Conditions.ScalingDownMessage);

            var primaryPods       = cluster.Status.Cluster.GetPrimaryNodes();
            var primariesToDelete = primaryPods.Count - cluster.Spec.NumberOfPrimaries;
            var primaryHosts      = primaryPods.Select(p => p.Pod.Spec.NodeName).ToHashSet();
            var podsPerHost       = primariesToDelete / primaryHosts.Count();

            foreach (var host in primaryHosts)
            {
                var hostPods = primaryPods.Where(p => p.Pod.Spec.NodeName == host).SelectRandom(podsPerHost);

                foreach (var pod in hostPods)
                {
                    await k8s.CoreV1.DeleteNamespacedPodAsync(pod.PodName, cluster.Metadata.NamespaceProperty);
                }
            }

            var replicaNodes     = cluster.Status.Cluster.GetReplicaNodes();
            var replicasToDelete = replicaNodes.Count - cluster.Spec.ReplicationFactor;
            var replicaHosts     = primaryPods.Select(p => p.Pod.Spec.NodeName).ToHashSet();
            podsPerHost          = replicasToDelete / replicaHosts.Count();

            foreach (var host in replicaHosts)
            {
                var hostPods = replicaNodes.Where(p => p.Pod.Spec.NodeName == host).SelectRandom(podsPerHost);

                foreach (var pod in hostPods)
                {
                    await k8s.CoreV1.DeleteNamespacedPodAsync(pod.PodName, cluster.Metadata.NamespaceProperty);
                }
            }
        }


        internal async Task DetachReplicaAsync(GarnetNode node, V1alpha1GarnetCluster resource)
        {

            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            foreach (var clusterNode in resource.Status.Cluster.Nodes.Where(n => n.Address != node.Address))
            {
                var garnet = new GarnetClient(clusterNode.Address, clusterNode.Port);

                if (clusterNode.Id.IsNullOrEmpty())
                {
                    clusterNode.Id = await garnet.MyIdAsync();
                }

                await garnet.ForgetAsync(clusterNode.Id);
            }

        }

        internal async Task ScaleUpAsync()
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            await cluster.SetConditionAsync(
                   k8s:     k8s,
                   type:    Constants.Conditions.Scaling,
                   status:  Constants.Conditions.StatusTrue,
                   reason:  Constants.Conditions.ScalingUpReason,
                   message: Constants.Conditions.ScalingUpMessage);

            var numPodsNeeded = NumPodsRequired() - cluster.Status.Cluster.NumberOfPods;
            var spec = CreatePodSpec();


            for (int i = 0; i < numPodsNeeded; i++)
            {
                var pod = new V1Pod().Initialize();

                pod.Metadata.EnsureLabels();
                pod.Metadata.EnsureAnnotations();
                    
                pod.Metadata.Name              = CreatePodName();
                pod.Metadata.NamespaceProperty = cluster.Metadata.NamespaceProperty;

                pod.Metadata.Labels.AddRange(cluster.Spec.AdditionalLabels);
                pod.Metadata.OwnerReferences.SetOwnerReference(cluster.MakeOwnerReference());
                pod.Metadata.Annotations.Add(Constants.Annotations.ManagedByKey, Constants.OperatorName);

                pod = await k8s.CoreV1.CreateNamespacedPodAsync(pod, cluster.Metadata.NamespaceProperty);

                cluster.Status.Cluster.Nodes.Add(new GarnetNode()
                {
                    Pod     = pod,
                    Role    = GarnetRole.None,
                    Port    = Constants.Ports.Redis,
                    PodName = pod.Metadata.Name,
                    Address = GetPodHeadlessAddress(pod),
                    Zone    = pod.Spec.NodeName
                });

            }
        }
        
        internal V1PodSpec CreatePodSpec()
        {
            return new V1PodSpec()
            {
                Containers =
                [
                    new V1Container()
                    {
                        Name            = Constants.GarnetContainer,
                        SecurityContext = cluster.Spec.SecurityContext,
                        Image           = cluster.Spec.Image.ToString(),
                        ImagePullPolicy = cluster.Spec.Image.PullPolicy,
                        Args =
                        [
                            "GarnetServer",
                            "--cluster",
                            "--bind", "0.0.0.0",
                            "--port", Constants.Ports.Redis.ToString()
                        ],
                        Ports = 
                        [
                            new V1ContainerPort(){
                                Name = Constants.Ports.RedisName,
                                ContainerPort = Constants.Ports.Redis,
                                Protocol = "tcp"
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

            if (cluster.Status.Cluster.NumberOfPods != cluster.Status.Cluster.NumberOfPodsReady)
            {
                return false;
            }

            if (cluster.Status.Cluster.NumberOfPods < numPodsRequired)
            {
                return true;
            }

            return false;
        }

        internal bool NeedsLessPods()
        {
            var numPodsRequired = NumPodsRequired();

            if (cluster.Status.Cluster.NumberOfPods != cluster.Status.Cluster.NumberOfPodsReady)
            {
                return false;
            }

            if (cluster.Status.Cluster.NumberOfPods > numPodsRequired)
            {
                return true;
            }

            return false;
        }

        internal int NumPodsRequired()
        {
            return cluster.Spec.NumberOfPrimaries * (1 + cluster.Spec.ReplicationFactor);
        }

        internal async Task<IEnumerable<V1Pod>> GetClusterPodsAsync()
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            var pods = await k8s.CoreV1.ListNamespacedPodAsync(namespaceParameter: cluster.Metadata.NamespaceProperty,
                    labelSelector: cluster.Spec.AdditionalLabels.ToLabelSelector());

            return pods.Items;
        }

        internal async Task ReconcileConfigMapAsync()
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            logger.LogInformationEx(() => $"Reconciling ConfigMap for {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");
            logger.LogInformationEx(() => $"Reconciled ConfigMap for {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");
        }

        internal async Task ReconcileServicesAsync()
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            logger.LogInformationEx(() => $"Reconciling Services for {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");

            await ReconcileServiceAsync(GetServiceName(), false);
            await ReconcileServiceAsync(GetHeadlessServiceName(), true);

            logger.LogInformationEx(() => $"Reconciled Services for {V1alpha1GarnetCluster.KubeKind}/{cluster.Name()}");
        }

        internal async Task ReconcileServiceAsync(string serviceName, bool headless)
        {
            await SyncContext.Clear;

            using var activity = TraceContext.ActivitySource?.StartActivity();

            var service = new V1Service();

            var exists = false;
            try
            {
                service = await k8s.CoreV1.ReadNamespacedServiceAsync(serviceName, cluster.Metadata.NamespaceProperty);
                exists = true;
            }
            catch (HttpOperationException e) when (e.Response.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                service                            = new V1Service().Initialize();
                service.Metadata.Name              = serviceName;
                service.Metadata.NamespaceProperty = cluster.Metadata.NamespaceProperty;
            }

            bool hasChanges = false;

            var spec = CreateServiceSpec(headless);
            spec.Selector.AddRange(cluster.Spec.AdditionalLabels);

            hasChanges = !spec.JsonEquals(service.Spec);
            hasChanges = service.Metadata.OwnerReferences.SetOwnerReference(cluster.MakeOwnerReference());

            if (!exists)
            {
                service.Spec = spec;
                await k8s.CoreV1.CreateNamespacedServiceAsync(service, serviceName, cluster.Metadata.NamespaceProperty);
                return;
            }

            if (hasChanges)
            {
                service.Spec = spec;
                await k8s.CoreV1.ReplaceNamespacedServiceAsync(service, serviceName, cluster.Metadata.NamespaceProperty);
                return;
            }
        }

        internal V1ServiceSpec CreateServiceSpec(bool headless = false)
        {
            var spec = new V1ServiceSpec()
            {
                Ports = new List<V1ServicePort>()
                    {
                        new V1ServicePort()
                        {
                            Name          = Constants.Ports.RedisName,
                            Protocol      = "TCP",
                            Port          = Constants.Ports.Redis,
                            TargetPort    = Constants.Ports.Redis,
                            AppProtocol   = "tcp",
                        },
                    },
                Selector              = new Dictionary<string, string>(),
                Type                  = "ClusterIP",
                InternalTrafficPolicy = "Cluster"

            };

            if (headless)
            {
                spec.ClusterIP        = "None";
            }

            return spec;
        }

        private string CreatePodName()
        {
            return (cluster.Spec.ServiceName ?? cluster.Name()) + "-" + NeonHelper.CreateBase36Uuid();
        }

        private string GetPodHeadlessAddress(V1Pod pod)
        {
            return pod.Status.PodIP.Replace(".", "-") + "." + GetHeadlessServiceName() + ".svc.cluster.local";
        }

        private string GetServiceName()
        {
            return cluster.Spec.ServiceName ?? cluster.Name();
        }

        private string GetHeadlessServiceName()
        {
            return $"{GetServiceName()}-headless";
        }
    }
}
