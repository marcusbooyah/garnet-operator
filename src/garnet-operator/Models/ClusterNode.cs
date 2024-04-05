using System;
using System.Collections.Generic;
using System.Linq;

namespace GarnetOperator.Models
{

    /// <summary>
    /// Represents a node in a cluster.
    /// </summary>
    public class ClusterNode
    {
        /// <summary>
        /// Gets or sets the unique identifier of the node.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets the IP address of the node.
        /// </summary>
        public string IpAddress { get; set; }

        /// <summary>
        /// Gets or sets the port number of the node.
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// Gets or sets the hostname of the node.
        /// </summary>
        public string Hostname { get; set; }

        /// <summary>
        /// Gets or sets the flags associated with the node.
        /// </summary>
        public IEnumerable<string> Flags { get; set; }

        /// <summary>
        /// Gets or sets the unique identifier of the master node.
        /// </summary>
        public string MasterId { get; set; }

        /// <summary>
        /// Gets or sets the number of ping sent to the node.
        /// </summary>
        public int PingSent { get; set; }

        /// <summary>
        /// Gets or sets the number of pong received from the node.
        /// </summary>
        public int PongReceived { get; set; }

        /// <summary>
        /// Gets or sets the configuration epoch of the node.
        /// </summary>
        public int ConfigEpoch { get; set; }

        /// <summary>
        /// Gets or sets the link state of the node.
        /// </summary>
        public string LinkState { get; set; }

        /// <summary>
        /// Gets or sets the list of slots assigned to the node.
        /// </summary>
        public List<int> Slots { get; set; }

        /// <summary>
        /// Creates a ClusterNode object from a RESP response.
        /// </summary>
        /// <param name="response">The RESP response string.</param>
        /// <returns>A ClusterNode object.</returns>
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
                Id = parts[0],
                IpAddress = ip,
                Port = port,
                Hostname = address.Split(",")[1],
                Flags = parts[2].Split(","),
                MasterId = master,
                PingSent = int.Parse(parts[4]),
                PongReceived = int.Parse(parts[5]),
                ConfigEpoch = int.Parse(parts[6]),
                LinkState = parts[7],
                Slots = slots
            };
        }
    }
}
