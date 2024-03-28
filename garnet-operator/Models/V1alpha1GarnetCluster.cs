using k8s;
using k8s.Models;

namespace GarnetOperator.Models
{
    [KubernetesEntityAttribute(Group = KubeGroup, Kind = KubeKind, ApiVersion = KubeVersion, PluralName = KubePlural)]
    public partial class V1alpha1GarnetCluster : 
        IKubernetesObject<V1ObjectMeta>, 
        ISpec<V1alpha1GarnetClusterSpec>, 
        IStatus<V1alpha1GarnetClusterStatus>
    {
        public const string KubeGroup   = "garnet.k8soperator.io";
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
    }
}