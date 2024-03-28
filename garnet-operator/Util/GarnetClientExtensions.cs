using System.Threading.Tasks;

using Garnet.client;

namespace GarnetOperator
{
    internal static partial class GarnetClientExtensions
    {
        public static Task<string> MeetAsync(this GarnetClient client, string address, int port = Constants.Ports.Redis)
            => client.ExecuteForStringResultAsync("CLUSTER", ["MEET", address, port.ToString()]);
        public static Task<string> ForgetAsync(this GarnetClient client, string id)
            => client.ExecuteForStringResultAsync("CLUSTER", ["FORGET", id]);
        public static Task<string> MyIdAsync(this GarnetClient client)
            => client.ExecuteForStringResultAsync("CLUSTER", ["MYID"]);
    }
}
