using System.Collections.Generic;
using System.Linq;

namespace GarnetOperator.Models
{
    public class Cluster
    {
        public string LabelSelectorPath { get; set; }

        public int? MaxReplicationFactor { get; set; }

        public int? MinReplicationFactor { get; set; }

        public List<GarnetNode> Nodes { get; set; }

        public string NodesPlacementInfo { get; set; }

        public int? NumberOfPods { get; set; }

        public int? NumberOfPodsReady { get; set; }

        public int? NumberOfPrimaries { get; set; }

        public int? NumberOfPrimariesReady { get; set; }

        public int? NumberOfGarnetNodesRunning { get; set; }

        public Dictionary<string, string> NumberOfReplicasPerPrimary { get; set; }

        public string Status { get; set; }

        public List<GarnetNode> GetPrimaryNodes()
        {
            return Nodes.Where(n => n.Role == GarnetRole.Primary).ToList();
        }

        public List<GarnetNode> GetReplicaNodes()
        {
            return Nodes.Where(n => n.Role == GarnetRole.Replica).ToList();
        }

        public List<GarnetNode> GetUnusedNodes()
        {
            return Nodes.Where(n => n.Role == GarnetRole.None).ToList();
        }

        public Dictionary<string, GarnetNode> GetPrimaryToReplicas()
        {
            var result = new Dictionary<string, GarnetNode>();
            foreach (var replica in GetReplicaNodes())
            {
                var nodeAdded = false;
                foreach (var primary in GetPrimaryNodes())
                {
                    if (replica.PrimaryId == primary.Id)
                    {
                        result[primary.Id] = replica;
                    }
                }
            }

            return result;
        }
    }
}