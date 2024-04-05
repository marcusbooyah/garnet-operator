using System.Collections.Generic;
using System.Linq;

namespace GarnetOperator.Models
{
    /// <summary>
    /// Represents a cluster in the Garnet Operator.
    /// </summary>
    public class Cluster
    {
        /// <summary>
        /// Gets or sets the label selector path for the cluster.
        /// </summary>
        public string LabelSelectorPath { get; set; }

        /// <summary>
        /// Gets or sets the maximum replication factor for the cluster.
        /// </summary>
        public int? MaxReplicationFactor { get; set; }

        /// <summary>
        /// Gets or sets the minimum replication factor for the cluster.
        /// </summary>
        public int? MinReplicationFactor { get; set; }

        /// <summary>
        /// Gets or sets the dictionary of nodes in the cluster.
        /// </summary>
        public Dictionary<string, GarnetNode> Nodes { get; set; } = new Dictionary<string, GarnetNode>();

        /// <summary>
        /// Gets or sets the nodes placement information for the cluster.
        /// </summary>
        public string NodesPlacementInfo { get; set; }

        /// <summary>
        /// Gets the number of pods in the cluster.
        /// </summary>
        public int NumberOfPods => Nodes.Count;

        /// <summary>
        /// Gets or sets the number of pods that are ready in the cluster.
        /// </summary>
        public int NumberOfPodsReady { get; set; }

        /// <summary>
        /// Gets the number of primary nodes in the cluster.
        /// </summary>
        public int NumberOfPrimaries => Nodes.Values.Where(n => n.Role == GarnetRole.Primary).Count();

        /// <summary>
        /// Gets the number of replica nodes in the cluster.
        /// </summary>
        public int NumberOfReplicas => Nodes.Values.Where(n => n.Role == GarnetRole.Replica).Count();

        /// <summary>
        /// Gets or sets the number of primary nodes that are ready in the cluster.
        /// </summary>
        public int NumberOfPrimariesReady { get; set; }

        /// <summary>
        /// Gets or sets the number of replica nodes that are ready in the cluster.
        /// </summary>
        public int NumberOfReplicasReady { get; set; }

        /// <summary>
        /// Gets the number of Garnet nodes running in the cluster.
        /// </summary>
        public int NumberOfGarnetNodesRunning => Nodes.Count;

        /// <summary>
        /// Gets or sets the dictionary of number of replicas per primary node in the cluster.
        /// </summary>
        public Dictionary<string, int> NumberOfReplicasPerPrimary { get; set; } = new Dictionary<string, int>();

        /// <summary>
        /// Gets or sets the status of the cluster.
        /// </summary>
        public string Status { get; set; }

        /// <summary>
        /// Gets a list of primary nodes in the cluster.
        /// </summary>
        /// <returns>A list of primary nodes.</returns>
        public List<GarnetNode> GetPrimaryNodes()
        {
            return Nodes.Values.Where(n => n.Role == GarnetRole.Primary).ToList();
        }

        /// <summary>
        /// Gets a list of replica nodes in the cluster.
        /// </summary>
        /// <returns>A list of replica nodes.</returns>
        public List<GarnetNode> GetReplicaNodes()
        {
            return Nodes.Values.Where(n => n.Role == GarnetRole.Replica).ToList();
        }

        /// <summary>
        /// Gets a list of unused nodes in the cluster.
        /// </summary>
        /// <returns>A list of unused nodes.</returns>
        public List<GarnetNode> GetUnusedNodes()
        {
            return Nodes.Values.Where(n => n.Role == GarnetRole.None).ToList();
        }

        /// <summary>
        /// Tries to remove a node from the cluster.
        /// </summary>
        /// <param name="uid">The UID of the node to remove.</param>
        public void TryRemoveNode(string uid)
        {
            if (Nodes.ContainsKey(uid))
            {
                Nodes.Remove(uid);
            }
        }
    }
}