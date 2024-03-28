using System.Collections.Generic;
using System.ComponentModel;

using k8s.Models;

namespace GarnetOperator.Models
{
    public class V1alpha1GarnetClusterSpec
    {
        public Dictionary<string, string> AdditionalLabels { get; set; }

        [DefaultValue(1)]
        public int NumberOfPrimaries { get; set; } = 1;

        [DefaultValue(1)]
        public int ReplicationFactor { get; set; } = 1;

        public RollingUpdate RollingUpdate { get; set; }

        public Scaling Scaling { get; set; }

        public string ServiceName { get; set; }

        [DefaultValue(false)]
        public bool ZoneAwareReplication { get; set; } = false;

        public V1SecurityContext SecurityContext { get; set; }

        public ImageSpec Image { get; set; }
        public V1ResourceRequirements Resources { get; set; }
        public Dictionary<string, string> NodeSelector { get; set; }

        public V1Affinity Affinity { get; set; }

        public List<V1Toleration> Tolerations { get; set; }
    }
}