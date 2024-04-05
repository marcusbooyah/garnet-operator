using k8s;
using k8s.KubeConfigModels;
using k8s.Models;

using Neon.Common;

namespace GarnetOperator.Models
{
    
    [KubernetesEntity(Group = KubeGroup, Kind = KubeKind, ApiVersion = KubeVersion, PluralName = KubePlural)]
    public partial class V1alpha1GarnetCluster :
        IKubernetesObject<V1ObjectMeta>,
        ISpec<V1alpha1GarnetClusterSpec>,
        IStatus<V1alpha1GarnetClusterStatus>
    {
        public const string KubeGroup   = Constants.KubernetesGroup;
        public const string KubeKind    = "GarnetCluster";
        public const string KubeVersion = "v1alpha1";
        public const string KubePlural  = "garnetclusters";

        public V1alpha1GarnetCluster()
        {
            ApiVersion = $"{KubeGroup}/{KubeVersion}";
            Kind = KubeKind;
        }

        /// <summary>
        /// The Kubernetes API version of the object.
        /// </summary>
        public string ApiVersion { get; set; }

        /// <summary>
        /// The Kubernetes kind of the object.
        /// </summary>
        public string Kind { get; set; }

        /// <summary>
        /// The metadata of the object.
        /// </summary>
        public V1ObjectMeta Metadata { get; set; }

        /// <summary>
        /// The specification of the GarnetCluster.
        /// </summary>
        public V1alpha1GarnetClusterSpec Spec { get; set; }

        /// <summary>
        /// The status of the GarnetCluster.
        /// </summary>
        public V1alpha1GarnetClusterStatus Status { get; set; }

        /// <summary>
        /// Creates a unique name for the pod based on the service name or metadata name and a base36 UUID.
        /// </summary>
        /// <returns>The unique pod name.</returns>
        public string CreatePodName()
        {
            return (Spec.ServiceName ?? Metadata.Name) + "-" + NeonHelper.CreateBase36Uuid();
        }

        /// <summary>
        /// Gets the service name in the format of "serviceName.namespace".
        /// </summary>
        /// <returns>The service name.</returns>
        public string GetServiceName()
        {
            return (Spec.ServiceName ?? Metadata.Name) + "." + Metadata.NamespaceProperty;
        }

        /// <summary>
        /// Gets the headless service name in the format of "serviceName-headless.namespace".
        /// </summary>
        /// <returns>The headless service name.</returns>
        public string GetHeadlessServiceName()
        {
            return (Spec.ServiceName ?? Metadata.Name) + "-headless" + "." + Metadata.NamespaceProperty;
        }

        /// <summary>
        /// Creates the pod address by combining the pod IP and the headless service name.
        /// </summary>
        /// <param name="pod">The V1Pod object.</param>
        /// <returns>The pod address.</returns>
        public string CreatePodAddress(V1Pod pod)
        {
            if (pod.Status == null 
            || pod.Status.PodIP.IsNullOrEmpty())
            {
                return null;
            }

            return $"{pod.Status.PodIP.Replace(".", "-")}.{GetHeadlessServiceName()}";
        }
    }
}