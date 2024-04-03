using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Neon.Common;

using Microsoft.AspNetCore.Http.HttpResults;

namespace GarnetOperator.Models
{
    public class ClusterInfo
    {
        public string State { get; set; }
        public int SlotsAssigned { get; set; }
        public int SlotsOk { get; set; }
        public int SlotsPreFail { get; set; }
        public int SlotsFail { get; set; }
        public int KnownNodes { get; set; }
        public int Size { get; set; }
        public int CurrentEpoch { get; set; }
        public int MyEpoch { get; set; }
        public int StatsMessagesSent { get; set; }
        public int StatsMessagesReceived { get; set; }

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
