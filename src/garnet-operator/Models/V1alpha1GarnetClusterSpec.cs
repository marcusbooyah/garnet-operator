using System.Collections.Generic;
using System.ComponentModel;

using k8s.Models;

namespace GarnetOperator.Models
{
    /// <summary>
    /// Represents the specification for a Garnet cluster.
    /// </summary>
    public class V1alpha1GarnetClusterSpec
    {
        /// <summary>
        /// Optionally define additional labels for the Garnet cluster.
        /// </summary>
        public Dictionary<string, string> AdditionalLabels { get; set; }

        /// <summary>
        /// The number of primary nodes in the Garnet cluster.
        /// </summary>
        [DefaultValue(1)]
        public int NumberOfPrimaries { get; set; } = 1;

        /// <summary>
        /// The replication factor for the Garnet cluster.
        /// </summary>
        [DefaultValue(1)]
        public int ReplicationFactor { get; set; } = 1;

        /// <summary>
        /// Additional Garnet arguments.
        /// </summary>
        public List<string> AdditionalArgs { get; set; }


        /// <summary>
        /// The service name for the Garnet cluster.
        /// </summary>
        public string ServiceName { get; set; }

        /// <summary>
        /// The security context for the Garnet cluster.
        /// </summary>
        public V1SecurityContext SecurityContext { get; set; }

        /// <summary>
        /// The image specification for the Garnet cluster.
        /// </summary>
        public ImageSpec Image { get; set; }

        /// <summary>
        /// The resource requirements for the Garnet cluster.
        /// </summary>
        public V1ResourceRequirements Resources { get; set; }

        /// <summary>
        /// The node selector for the Garnet cluster.
        /// </summary>
        public Dictionary<string, string> NodeSelector { get; set; }

        /// <summary>
        /// The affinity for the Garnet cluster.
        /// </summary>
        public V1Affinity Affinity { get; set; }

        /// <summary>
        /// The tolerations for the Garnet cluster.
        /// </summary>
        public List<V1Toleration> Tolerations { get; set; }

        /// <summary>
        /// The topology spread constraints for the Garnet cluster.
        /// </summary>
        public List<V1TopologySpreadConstraint> TopologySpreadConstraints { get; set; }
    }
}