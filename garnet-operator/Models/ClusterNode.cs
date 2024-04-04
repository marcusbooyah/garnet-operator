using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Neon.Common;

using Microsoft.AspNetCore.Http.HttpResults;

namespace GarnetOperator.Models
{
    public class ClusterNode
    {
        public string Id { get; set; }
        public string IpAddress { get; set; }
        public int Port { get; set; }
        public string Hostname { get; set; }
        public IEnumerable<string> Flags { get; set; }
        public string MasterId { get; set; }
        public int PingSent { get; set; }
        public int PongReceived { get; set; }
        public int ConfigEpoch { get; set; }
        public string LinkState { get; set; }
        public List<int> Slots { get; set; }

        public static ClusterNode FromRespResponse(string response)
        {
            var parts = response.Split(" ");
            var address = parts[1];
            var ip = address.Split(":")[0];
            var port = int.Parse(address.Split(":")[1].Split("@")[0]);

            string master = null;
            if (parts[3] != "-")
            {
                master = parts[3];
            }

            var slots = new List<int>();

            if (parts.Count() > 8)
            {

                for (int i = 8; i < parts.Count(); i++)
                {
                    slots.AddRange(parts[i].Split("-").Select(x => int.Parse(x)));
                }
            }

            return new ClusterNode()
            {
                Id           = parts[0],
                IpAddress    = ip,
                Port         = port,
                Hostname     = address.Split(",")[1],
                Flags        = parts[2].Split(","),
                MasterId     = master,
                PingSent     = int.Parse(parts[4]),
                PongReceived = int.Parse(parts[5]),
                ConfigEpoch  = int.Parse(parts[6]),
                LinkState    = parts[7],
                Slots        = slots
            };
        }
    }
}
