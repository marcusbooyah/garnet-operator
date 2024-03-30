using System.Collections.Generic;
using System.Linq;

namespace GarnetOperator.Models
{
    public class Cluster
    {
        public string LabelSelectorPath { get; set; }

        public int? MaxReplicationFactor { get; set; }

        public int? MinReplicationFactor { get; set; }

        public Dictionary<string, GarnetNode> Nodes { get; set; } = new Dictionary<string, GarnetNode>();

        public string NodesPlacementInfo { get; set; }

        public int NumberOfPods => Nodes.Count;

        public int NumberOfPodsReady { get; set; }

        public int NumberOfPrimaries => Nodes.Values.Where(n => n.Role == GarnetRole.Primary).Count();

        public int NumberOfReplicas => Nodes.Values.Where(n => n.Role == GarnetRole.Replica).Count();

        public int NumberOfPrimariesReady { get; set; }

        public int NumberOfGarnetNodesRunning => Nodes.Count;

        public Dictionary<string, int> NumberOfReplicasPerPrimary { get; set; } = new Dictionary<string, int>();

        public string Status { get; set; }

        public List<GarnetNode> GetPrimaryNodes()
        {
            return Nodes.Values.Where(n => n.Role == GarnetRole.Primary).ToList();
        }

        public List<GarnetNode> GetReplicaNodes()
        {
            return Nodes.Values.Where(n => n.Role == GarnetRole.Replica).ToList();
        }

        public List<GarnetNode> GetUnusedNodes()
        {
            return Nodes.Values.Where(n => n.Role == GarnetRole.None).ToList();
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

        public void TryRemoveNode(string uid)
        {
            if (Nodes.ContainsKey(uid))
            {

                Nodes.Remove(uid);
            }
        }
    }
}