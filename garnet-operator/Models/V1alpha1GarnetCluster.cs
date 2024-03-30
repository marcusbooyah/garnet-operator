using k8s;
using k8s.KubeConfigModels;
using k8s.Models;

using Neon.Common;

namespace GarnetOperator.Models
{
    [KubernetesEntityAttribute(Group = KubeGroup, Kind = KubeKind, ApiVersion = KubeVersion, PluralName = KubePlural)]
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

        public string ApiVersion { get; set; }
        public string Kind { get; set; }
        public V1ObjectMeta Metadata { get; set; }
        public V1alpha1GarnetClusterSpec Spec { get; set; }
        public V1alpha1GarnetClusterStatus Status { get; set; }
        public string CreatePodName()
        {
            return (Spec.ServiceName ?? Metadata.Name) + "-" + NeonHelper.CreateBase36Uuid();
        }
        public string GetServiceName()
        {
            return (Spec.ServiceName ?? Metadata.Name) + "." + Metadata.NamespaceProperty;
        }

        public string GetHeadlessServiceName()
        {
            return (Spec.ServiceName ?? Metadata.Name) + "-headless" + "." + Metadata.NamespaceProperty;
        }

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