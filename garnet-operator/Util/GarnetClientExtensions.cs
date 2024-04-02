using System;
using System.Threading.Tasks;

using Garnet.client;

using IdentityModel.OidcClient;

namespace GarnetOperator
{
    internal static partial class GarnetClientExtensions
    {
        public const string OkResult = "OK";

        /// <summary>
        /// Sends a MEET command to the Garnet client to connect to a specified address and port.
        /// </summary>
        /// <param name="client">The Garnet client.</param>
        /// <param name="address">The address to connect to.</param>
        /// <param name="port">The port to connect to. Defaults to Constants.Ports.Redis.</param>
        /// <returns>The result of the MEET command.</returns>
        public static async Task<string> MeetAsync(this GarnetClient client, string address, int port = Constants.Ports.Redis)
        {
            var result = await client.ExecuteForStringResultAsync("CLUSTER", ["MEET", address, port.ToString()]);

            EnsureSuccess(result);

            return result;
        }

        /// <summary>
        /// Sends a FORGET command to the Garnet client to remove a node from the cluster.
        /// </summary>
        /// <param name="client">The Garnet client.</param>
        /// <param name="id">The ID of the node to remove.</param>
        /// <returns>The result of the FORGET command.</returns>
        public static async Task<string> ForgetAsync(this GarnetClient client, string id)
        {
            try
            {
                var result = await client.ExecuteForStringResultAsync("CLUSTER", ["FORGET", id]);

                return result;
            }
            catch (Exception e)
            {
                if (e.Message.StartsWith("ERR I don't know about node"))
                {
                    return string.Empty;
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Sends a MYID command to the Garnet client to retrieve the ID of the current node.
        /// </summary>
        /// <param name="client">The Garnet client.</param>
        /// <returns>The result of the MYID command.</returns>
        public static async Task<string> MyIdAsync(this GarnetClient client)
        {
            var result = await client.ExecuteForStringResultAsync("CLUSTER", ["MYID"]);

            return result;
        }

        /// <summary>
        /// Sends a REPLICATE command to the Garnet client to replicate a specified node.
        /// </summary>
        /// <param name="client">The Garnet client.</param>
        /// <param name="id">The ID of the node to replicate.</param>
        /// <returns>The result of the REPLICATE command.</returns>
        public static async Task<string> ReplicateAsync(this GarnetClient client, string id)
        {
            var result = await client.ExecuteForStringResultAsync("CLUSTER", ["REPLICATE", id]);

            EnsureSuccess(result);

            return result;
        }

        /// <summary>
        /// Sends a REPLICAOF command to the Garnet client to detach the replica.
        /// </summary>
        /// <param name="client">The Garnet client.</param>
        /// <returns>The result of the REPLICAOF command.</returns>
        public static async Task<string> DetachReplicaAsync(this GarnetClient client)
        {
            var result = await client.ExecuteForStringResultAsync("REPLICAOF", ["NO", "ONE"]);

            EnsureSuccess(result);

            return result;
        }
        public static async Task<string> SetConfigEpochAsync(this GarnetClient client, int epoch)
        {
            var result = await client.ExecuteForStringResultAsync("CLUSTER", ["SET-CONFIG-EPOCH", epoch.ToString()]);

            EnsureSuccess(result);

            return result;
        }

        public static async Task<string> MigrateSlotsRangeAsync(
            this GarnetClient client,
            string address,
            int port,
            int start,
            int end,
            string key = null,
            int timeout = 0,
            int database = -1)
        {
            key ??= string.Empty;

            var result = await client.ExecuteForStringResultAsync(
                op: "CLUSTER",
                args:
                [
                    "MIGRATE",
                    $@"""{key}""",
                    timeout.ToString(),
                    database.ToString(),
                    "SLOTSRANGE",
                    start.ToString(),
                    end.ToString()
                ]);

            EnsureSuccess(result);

            return result;

        }



        public static async Task<string> AddSlotsRangeAsync(
            this GarnetClient client,
            string            address,
            int               port,
            int               start,
            int               end)
        {
            var result = await client.ExecuteForStringResultAsync(
                op: "CLUSTER",
                args:
                [
                    "ADDSLOTSRANGE",
                    start.ToString(),
                    end.ToString()
                ]);

            EnsureSuccess(result);

            return result;
        }

            // CLUSTER ADDSLOTSRANGE start-slot end-slot

        private static void EnsureSuccess(string value)
        {
            if (value != OkResult)
            {
                throw new Exception("garnet error");
            }
        }
    }
}
