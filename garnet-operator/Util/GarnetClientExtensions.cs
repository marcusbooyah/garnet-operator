using System;
using System.Threading.Tasks;

using Garnet.client;

using IdentityModel.OidcClient;

namespace GarnetOperator
{
    internal static partial class GarnetClientExtensions
    {
        public const string OkResult = "OK";
        public static async Task<string> MeetAsync(this GarnetClient client, string address, int port = Constants.Ports.Redis)
        {
            var result = await client.ExecuteForStringResultAsync("CLUSTER", ["MEET", address, port.ToString()]);

            EnsureSuccess(result);

            return result;
        }
        public static async Task<string> ForgetAsync(this GarnetClient client, string id)
        {
            var result = await client.ExecuteForStringResultAsync("CLUSTER", ["FORGET", id]);

            EnsureSuccess(result);

            return result;
        }
        public static async Task<string> MyIdAsync(this GarnetClient client)
        {
            var result = await client.ExecuteForStringResultAsync("CLUSTER", ["MYID"]);

            return result;
        }
        public static async Task<string> ReplicateAsync(this GarnetClient client, string id)
        {
            var result = await client.ExecuteForStringResultAsync("CLUSTER", ["REPLICATE", id]);

            EnsureSuccess(result);

            return result;
        }
        public static async Task<string> DetachReplicaAsync(this GarnetClient client)
        {
            var result = await client.ExecuteForStringResultAsync("REPLICAOF", ["NO", "ONE"]);

            EnsureSuccess(result);

            return result;
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
