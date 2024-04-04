using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Garnet.client;

using GarnetOperator.Models;

using Neon.Common;

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
        public static async Task<string> MeetAsync(
            this GarnetClient client,
            string address,
            int port = Constants.Ports.Redis,
            CancellationToken cancellationToken = default)
        {
            var result = await client.ExecuteForStringResultWithCancellationAsync(
                op: "CLUSTER",
                args: ["MEET", address, port.ToString()],
                token: cancellationToken);

            EnsureSuccess(result);

            return result;
        }

        /// <summary>
        /// Sends a FORGET command to the Garnet client to remove a node from the cluster.
        /// </summary>
        /// <param name="client">The Garnet client.</param>
        /// <param name="id">The ID of the node to remove.</param>
        /// <returns>The result of the FORGET command.</returns>
        public static async Task<string> ForgetAsync(
            this GarnetClient client,
            string id,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var result = await client.ExecuteForStringResultWithCancellationAsync(
                    op: "CLUSTER",
                    args: ["FORGET", id],
                    token: cancellationToken);

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
        public static async Task<string> MyIdAsync(this GarnetClient client, CancellationToken cancellationToken = default)
        {
            var result = await client.ExecuteForStringResultWithCancellationAsync(
                op: "CLUSTER",
                args: ["MYID"],
                token: cancellationToken);

            return result;
        }
        public static async Task<ClusterInfo> ClusterInfoAsync(this GarnetClient client, CancellationToken cancellationToken = default)
        {
            var resp = await client.ExecuteForStringResultWithCancellationAsync(
                op: "CLUSTER",
                args: ["INFO"],
                token: cancellationToken);

            var result = ClusterInfo.FromRespResponse(resp);

            return result;
        }

        public static async Task<string> FlushAllAsync(this GarnetClient client, CancellationToken cancellationToken = default)
        {
            var result = await client.ExecuteForStringResultWithCancellationAsync(op: "FLUSHALL", token: cancellationToken);

            return result;
        }

        public static async Task<string> ClusterResetAsync(
            this GarnetClient client,
            bool hard = false,
            CancellationToken cancellationToken = default)
        {
            var result = await client.ExecuteForStringResultWithCancellationAsync(
                op: "CLUSTER",
                args: ["RESET", hard ? "HARD" : "SOFT"],
                token: cancellationToken);

            return result;
        }

        /// <summary>
        /// Sends a REPLICATE command to the Garnet client to replicate a specified node.
        /// </summary>
        /// <param name="client">The Garnet client.</param>
        /// <param name="id">The ID of the node to replicate.</param>
        /// <returns>The result of the REPLICATE command.</returns>
        public static async Task<string> ReplicateAsync(
            this GarnetClient client,
            string id,
            CancellationToken cancellationToken = default)
        {
            var success = false;
            string result = null;
            var tries = 0;

            while (!success && !cancellationToken.IsCancellationRequested && tries < 10)
            {
                try
                {
                    result = await client.ExecuteForStringResultWithCancellationAsync(
                        op: "CLUSTER",
                        args: ["REPLICATE", id],
                        token: cancellationToken);

                    EnsureSuccess(result);
                    success = true;
                }
                catch (Exception e)
                {
                    if (e.Message == $"ERR I don't know about node {id}.")
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1));
                    }
                    else
                    {
                        throw;
                    }
                }
            }


            return result;
        }

        /// <summary>
        /// Sends a REPLICAOF NO ONE command to the Garnet client to detach the replica.
        /// </summary>
        /// <param name="client">The Garnet client.</param>
        /// <returns>The result of the REPLICAOF command.</returns>
        public static async Task<string> DetachReplicaAsync(this GarnetClient client, CancellationToken cancellationToken = default)
        {
            var result = await client.ExecuteForStringResultWithCancellationAsync(
                op: "REPLICAOF",
                args: ["NO", "ONE"],
                token: cancellationToken);

            EnsureSuccess(result);

            return result;
        }
        public static async Task<string> SetConfigEpochAsync(
            this GarnetClient client,
            int epoch,
            CancellationToken cancellationToken = default)
        {
            var result = await client.ExecuteForStringResultWithCancellationAsync(
                op: "CLUSTER",
                args: ["SET-CONFIG-EPOCH", epoch.ToString()],
                token: cancellationToken);

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
            int database = -1,
            CancellationToken cancellationToken = default)
        {
            key ??= string.Empty;

            var argString = string.Join(" ", [
                    address,
                    port.ToString(),
                    key,
                    timeout.ToString(),
                    database.ToString(),
                    "SLOTSRANGE",
                    start.ToString(),
                    end.ToString()
                ]);

            var result = await client.ExecuteForStringResultWithCancellationAsync(
                op: "MIGRATE",
                args:
                [
                    address,
                    port.ToString(),
                    key,
                    timeout.ToString(),
                    database.ToString(),
                    "SLOTSRANGE",
                    start.ToString(),
                    end.ToString()
                ],
                token: cancellationToken);

            EnsureSuccess(result);

            return result;

        }

        public static async Task<string> AddSlotsRangeAsync(
            this GarnetClient client,
            int start,
            int end,
            CancellationToken cancellationToken = default)
        {
            var result = await client.ExecuteForStringResultWithCancellationAsync(
                op: "CLUSTER",
                args:
                [
                    "ADDSLOTSRANGE",
                    start.ToString(),
                    end.ToString()
                ],
                token: cancellationToken);

            EnsureSuccess(result);

            return result;
        }

        public static async Task<IEnumerable<ClusterNode>> GetNodesAsync(
            this GarnetClient client,
            CancellationToken cancellationToken = default)
        {
            var resp = await client.ExecuteForStringResultWithCancellationAsync(
                op:    "CLUSTER",
                args:  ["NODES"],
                token: cancellationToken);

            var result = new List<ClusterNode>();

            foreach (var line in resp.ToLines())
            {
                result.Add(ClusterNode.FromRespResponse(line));
            }

            return result;
        }

        public static async Task<ClusterNode> GetSelfAsync(
            this GarnetClient client,
            CancellationToken cancellationToken = default)
        {
            var resp = await client.ExecuteForStringResultWithCancellationAsync(
                op:    "CLUSTER",
                args:  ["NODES"],
                token: cancellationToken);

            var result = new List<ClusterNode>();

            foreach (var line in resp.ToLines())
            {
                result.Add(ClusterNode.FromRespResponse(line));
            }

            return result.Where(n => n.Flags.Contains("myself")).FirstOrDefault();
        }

        private static void EnsureSuccess(string value)
        {
            if (value != OkResult)
            {
                throw new Exception("garnet error");
            }
        }
    }
}
