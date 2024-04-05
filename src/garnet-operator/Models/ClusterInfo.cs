using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Neon.Common;

using Microsoft.AspNetCore.Http.HttpResults;

namespace GarnetOperator.Models
{

    /// <summary>
    /// Represents the information about a cluster.
    /// </summary>
    public class ClusterInfo
    {
        /// <summary>
        /// Gets or sets the state of the cluster.
        /// </summary>
        public string State { get; set; }

        /// <summary>
        /// Gets or sets the number of slots assigned in the cluster.
        /// </summary>
        public int SlotsAssigned { get; set; }

        /// <summary>
        /// Gets or sets the number of slots that are in OK state in the cluster.
        /// </summary>
        public int SlotsOk { get; set; }

        /// <summary>
        /// Gets or sets the number of slots that are in pre-fail state in the cluster.
        /// </summary>
        public int SlotsPreFail { get; set; }

        /// <summary>
        /// Gets or sets the number of slots that are in fail state in the cluster.
        /// </summary>
        public int SlotsFail { get; set; }

        /// <summary>
        /// Gets or sets the number of known nodes in the cluster.
        /// </summary>
        public int KnownNodes { get; set; }

        /// <summary>
        /// Gets or sets the size of the cluster.
        /// </summary>
        public int Size { get; set; }

        /// <summary>
        /// Gets or sets the current epoch of the cluster.
        /// </summary>
        public int CurrentEpoch { get; set; }

        /// <summary>
        /// Gets or sets the epoch of the cluster for this instance.
        /// </summary>
        public int MyEpoch { get; set; }

        /// <summary>
        /// Gets or sets the number of stats messages sent by the cluster.
        /// </summary>
        public int StatsMessagesSent { get; set; }

        /// <summary>
        /// Gets or sets the number of stats messages received by the cluster.
        /// </summary>
        public int StatsMessagesReceived { get; set; }

        /// <summary>
        /// Creates a new <see cref="ClusterInfo"/> instance from the response string.
        /// </summary>
        /// <param name="response">The response string.</param>
        /// <returns>A new <see cref="ClusterInfo"/> instance.</returns>
        public static ClusterInfo FromRespResponse(string response)
        {
            return new ClusterInfo()
            {
                State                 = GetStringValue("cluster_state", response),
                SlotsAssigned         = GetIntValue("cluster_slots_assigned", response),
                SlotsOk               = GetIntValue("cluster_slots_ok", response),
                SlotsPreFail          = GetIntValue("cluster_slots_pfail", response),
                SlotsFail             = GetIntValue("cluster_slots_fail", response),
                KnownNodes            = GetIntValue("cluster_known_nodes", response),
                Size                  = GetIntValue("cluster_size", response),
                CurrentEpoch          = GetIntValue("cluster_current_epoch", response),
                MyEpoch               = GetIntValue("cluster_my_epoch", response),
                StatsMessagesSent     = GetIntValue("cluster_stats_messages_sent", response),
                StatsMessagesReceived = GetIntValue("cluster_stats_messages_received", response),
            };
        }

        /// <summary>
        /// Gets the integer value associated with the specified key from the response.
        /// </summary>
        /// <param name="key">The key to search for in the response.</param>
        /// <param name="response">The response string.</param>
        /// <returns>The integer value associated with the key, or the default value if the key is not found.</returns>
        public static int GetIntValue(string key, string response)
        {
            var line = response
                    .ToLines()
                    .Where(s => s.StartsWith(key))
                    .FirstOrDefault();

            if (line == null)
            {
                return default;
            }

            var value = line.Split(":").Last();

            return int.Parse(value);
        }

        /// <summary>
        /// Gets the string value associated with the specified key from the response.
        /// </summary>
        /// <param name="key">The key to search for in the response.</param>
        /// <param name="response">The response string.</param>
        /// <returns>The string value associated with the key, or the default value if the key is not found.</returns>
        public static string GetStringValue(string key, string response)
        {
            var line = response
                    .ToLines()
                    .Where(s => s.StartsWith(key))
                    .FirstOrDefault();

            if (line == null)
            {
                return default;
            }

            var value = line.Split(":").Last();

            return value;
        }
    }
}
