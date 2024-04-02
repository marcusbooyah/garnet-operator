using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Xml.Linq;

using Garnet.client;

using GarnetOperator.Models;

using k8s;
using k8s.Models;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Neon.Common;
using Neon.Diagnostics;
using Neon.K8s.PortForward;
using Neon.Net;
using Neon.Tasks;

namespace GarnetOperator.Util
{
    /// <summary>
    /// Helper class for interacting with Garnet.
    /// </summary>
    public class GarnetHelper
    {
        private readonly IKubernetes                      k8s;
        private readonly ILogger<GarnetHelper>            logger;
        private readonly ILoggerFactory                   loggerFactory;
        private readonly IServiceProvider                 services;
        private readonly Dictionary<string, GarnetClient> clients;

        /// <summary>
        /// Initializes a new instance of the <see cref="GarnetHelper"/> class.
        /// </summary>
        /// <param name="k8s">The Kubernetes client.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="loggerFactory">The logger factory.</param>
        /// <param name="services">The service provider.</param>
        public GarnetHelper(
           IKubernetes           k8s,
           ILogger<GarnetHelper> logger,
           ILoggerFactory        loggerFactory,
           IServiceProvider      services)
        {
            this.k8s           = k8s;
            this.logger        = logger;
            this.loggerFactory = loggerFactory;
            this.services      = services;
            this.clients       = new Dictionary<string, GarnetClient>();
        }

        /// <summary>
        /// Creates a Garnet client asynchronously using the specified GarnetNode.
        /// </summary>
        /// <param name="node">The GarnetNode containing the connection details.</param>
        /// <returns>A task representing the asynchronous operation. The task result contains the GarnetClient.</returns>
        public Task<GarnetClient> CreateClientAsync(GarnetNode node)
        {
            return CreateClientAsync(node.Address, node.Port, node.Namespace, node.PodName);
        }

        /// <summary>
        /// Creates a Garnet client asynchronously using the specified V1Pod and port.
        /// </summary>
        /// <param name="pod">The V1Pod object representing the pod.</param>
        /// <param name="port">The port number.</param>
        /// <returns>A task representing the asynchronous operation. The task result contains the GarnetClient.</returns>
        public Task<GarnetClient> CreateClientAsync(V1Pod pod, int port)
        {
            return CreateClientAsync(pod.Status.PodIP, port, pod.Namespace(), pod.Name());
        }

        /// <summary>
        /// Creates a Garnet client asynchronously using the specified address, port, namespace, and pod name.
        /// </summary>
        /// <param name="address">The address of the Garnet node.</param>
        /// <param name="port">The port number.</param>
        /// <param name="namespace">The namespace of the pod.</param>
        /// <param name="podName">The name of the pod.</param>
        /// <returns>A task representing the asynchronous operation. The task result contains the GarnetClient.</returns>
        public async Task<GarnetClient> CreateClientAsync(
            string address,
            int    port,
            string @namespace,
            string podName)
        {
            await SyncContext.Clear;

            var key = CreateKey(podName, @namespace);

            if (clients.TryGetValue(key, out var cachedClient))
            {
                if (!cachedClient.IsConnected)
                {
                    await cachedClient.ConnectAsync();
                }

                return cachedClient;
            }

            var clusterHost = $"{address}.{@namespace}";
            var clusterPort = port;

            logger?.LogInformationEx(() => $"Connecting to node: {podName} at: [{clusterHost}:{clusterPort}]");

            if (NeonHelper.IsDevWorkstation)
            {
                var portManager = this.services.GetRequiredService<PortForwardManager>();

                var localPort = NetHelper.GetUnusedTcpPort();
                portManager.StartPodPortForward(podName, @namespace, localPort, clusterPort);

                clusterHost = "localhost";
                clusterPort = localPort;
            }

            var client = new GarnetClient(clusterHost, clusterPort, logger: logger);

            if (!NeonHelper.IsDevWorkstation)
            {
                clients.Add(key, client);
            }

            await client.ConnectAsync();

            return client;
        }

        /// <summary>
        /// Executes a Redis command asynchronously using the specified GarnetNode.
        /// </summary>
        /// <param name="node">The GarnetNode containing the connection details.</param>
        /// <param name="json">A flag indicating whether the command should return the result in JSON format.</param>
        /// <param name="command">The Redis command to execute.</param>
        /// <returns>A task representing the asynchronous operation. The task result contains the Redis command result.</returns>
        public Task<string> ExecuteRedisCommandAsync(GarnetNode node, bool json, params string[] command)
        {
            return ExecuteRedisCommandAsync(node.Address, node.Port, node.Namespace, node.PodName, json, command);
        }

        /// <summary>
        /// Executes a Redis command asynchronously using the specified address, port, namespace, pod name, and command.
        /// </summary>
        /// <param name="address">The address of the Garnet node.</param>
        /// <param name="port">The port number.</param>
        /// <param name="namespace">The namespace of the pod.</param>
        /// <param name="podName">The name of the pod.</param>
        /// <param name="json">A flag indicating whether the command should return the result in JSON format.</param>
        /// <param name="command">The Redis command to execute.</param>
        /// <returns>A task representing the asynchronous operation. The task result contains the Redis command result.</returns>
        public async Task<string> ExecuteRedisCommandAsync(
            string address,
            int port,
            string @namespace,
            string podName,
            bool json = true,
            params string[] command)
        {
            await SyncContext.Clear;

            var clusterHost = $"{address}.{@namespace}";
            var clusterPort = port;

            logger?.LogInformationEx(() => $"Connecting to node: {podName} at: [{clusterHost}:{clusterPort}]");

            var cmd = new List<string>();

            if (NeonHelper.IsDevWorkstation)
            {
                cmd.Add("wsl");

                var portManager = this.services.GetRequiredService<PortForwardManager>();

                var localPort = NetHelper.GetUnusedTcpPort();
                var hostname = Dns.GetHostName();
                var hostEntry = await Dns.GetHostEntryAsync(hostname);
                var myIP = hostEntry.AddressList.Last().ToString();
                portManager.StartPodPortForward(podName, @namespace, localPort, clusterPort, localAddress: IPAddress.Parse(myIP));

                clusterHost = myIP;
                clusterPort = localPort;
            }
            cmd.Add("redis-cli");

            if (json == true)
            {
                cmd.Add("--json");
            };

            cmd.AddRange(
            [
                "-h", clusterHost,
                "-p", clusterPort.ToString()
            ]);


            cmd.AddRange(command);

            var response = await NeonHelper.ExecuteCaptureAsync(cmd.First(), cmd.Skip(1).ToArray());

            return response.OutputText;
        }

        /// <summary>
        /// Retrieves the list of shards asynchronously using the specified GarnetNode.
        /// </summary>
        /// <param name="node">The GarnetNode containing the connection details.</param>
        /// <returns>A task representing the asynchronous operation. The task result contains the list of shards.</returns>
        public async Task<List<Shard>> GetShardsAsync(GarnetNode node)
        {
            var result = await ExecuteRedisCommandAsync(node, true, "cluster", "shards");

            return JsonSerializer.Deserialize<ShardList>(result).Shards;

        }

        private static string CreateKey(string podName, string podNamespace)
        {
            return $"{podName}.{podNamespace}";
        }
    }
}
