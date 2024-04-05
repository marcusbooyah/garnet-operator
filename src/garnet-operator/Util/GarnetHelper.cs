using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

using Garnet.client;

using GarnetOperator.Models;

using k8s;
using k8s.KubeConfigModels;
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
        public Task<GarnetClient> CreateClientAsync(GarnetNode node, CancellationToken cancellationToken = default)
        {
            return CreateClientAsync(node.Address, node.Port, node.Namespace, node.PodName, cancellationToken);
        }

        /// <summary>
        /// Creates a Garnet client asynchronously using the specified V1Pod and port.
        /// </summary>
        /// <param name="pod">The V1Pod object representing the pod.</param>
        /// <param name="port">The port number.</param>
        /// <returns>A task representing the asynchronous operation. The task result contains the GarnetClient.</returns>
        public Task<GarnetClient> CreateClientAsync(V1Pod pod, int port, string serviceName, CancellationToken cancellationToken = default)
        {
            var address = $"{pod.Status.PodIP.Replace(".", "-")}.{serviceName}";

            return CreateClientAsync(address, port, pod.Namespace(), pod.Name(), cancellationToken);
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
            string podName,
            CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            var clusterHost = $"{address}.{@namespace}.svc.cluster.local";
            var clusterPort = port;

            logger?.LogInformationEx(() => $"Connecting to node: {podName} at: [{clusterHost}:{clusterPort}]");

            var key = CreateKey(podName, @namespace);

            if (clients.TryGetValue(key, out var cachedClient))
            {
                logger?.LogInformationEx(() => $"Returning cached client");

                if (!cachedClient.IsConnected)
                {
                    logger?.LogInformationEx(() => $"Reconnecting cached client");

                    await cachedClient.ConnectAsync();
                }

                return cachedClient;
            }


            if (NeonHelper.IsDevWorkstation)
            {
                V1Service npSvc = null;
                try
                {
                    npSvc = await k8s.CoreV1.ReadNamespacedServiceAsync(podName, @namespace, cancellationToken: cancellationToken);
                }
                catch
                {
                    var pod = await k8s.CoreV1.ReadNamespacedPodAsync(podName, @namespace, cancellationToken: cancellationToken);

                    var svc = new V1Service().Initialize();
                    svc.Metadata.Name              = podName;
                    svc.Metadata.NamespaceProperty = @namespace;
                    svc.Spec                       = new V1ServiceSpec()
                    {
                        Type = "NodePort",
                        Ports =
                        [
                            new V1ServicePort(){
                                Name       = "redis",
                                Protocol   = "TCP",
                                Port       = Constants.Ports.Redis,
                                TargetPort = Constants.Ports.Redis
                            }
                        ],

                    };

                    svc.Spec.Selector = new Dictionary<string, string>();
                    svc.Spec.Selector.Add(Constants.Labels.PodName, podName);

                    svc.Metadata.SetOwnerReference(new V1OwnerReference()
                    {
                        ApiVersion = "garnet.k8soperator.io/v1alpha1",
                        Kind       = "GarnetCluster",
                        Name       = pod.GetLabel(Constants.Labels.ClusterName),
                        Uid        = pod.GetLabel(Constants.Labels.ClusterId),
                    });

                    npSvc = await k8s.CoreV1.CreateNamespacedServiceAsync(svc, @namespace, cancellationToken: cancellationToken);
                }
                clusterHost = "10.100.42.100";
                clusterPort = npSvc.Spec.Ports.First().NodePort.Value;
            }

            var client = new GarnetClient(clusterHost, clusterPort, logger: logger);

            if (!NeonHelper.IsDevWorkstation)
            {
                clients.Add(key, client);
            }

            await client.ConnectAsync(cancellationToken);

            return client;
        }

        /// <summary>
        /// Executes a Redis command asynchronously using the specified GarnetNode.
        /// </summary>
        /// <param name="node">The GarnetNode containing the connection details.</param>
        /// <param name="json">A flag indicating whether the command should return the result in JSON format.</param>
        /// <param name="command">The Redis command to execute.</param>
        /// <returns>A task representing the asynchronous operation. The task result contains the Redis command result.</returns>
        public Task<string> ExecuteRedisCommandAsync(GarnetNode node, bool json, CancellationToken cancellationToken = default, params string[] command)
        {
            return ExecuteRedisCommandAsync(node.Address, node.Port, node.Namespace, node.PodName, json, cancellationToken, command);
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
            CancellationToken cancellationToken = default,
            params string[] command)
        {
            await SyncContext.Clear;

            var clusterHost = $"{address}.{@namespace}.svc.cluster.local";
            var clusterPort = port;

            logger?.LogInformationEx(() => $"Connecting to node: {podName} at: [{clusterHost}:{clusterPort}]");

            var cmd = new List<string>();

            if (NeonHelper.IsDevWorkstation)
            {
                cmd.Add("wsl");

                V1Service npSvc = null;
                try
                {
                    npSvc = await k8s.CoreV1.ReadNamespacedServiceAsync(podName, @namespace, cancellationToken: cancellationToken);
                }
                catch
                {
                    var pod = await k8s.CoreV1.ReadNamespacedPodAsync(podName, @namespace, cancellationToken: cancellationToken);

                    var svc = new V1Service().Initialize();
                    svc.Metadata.Name = podName;
                    svc.Metadata.NamespaceProperty = @namespace;
                    svc.Spec = new V1ServiceSpec()
                    {
                        Type = "NodePort",
                        Ports =
                        [
                            new V1ServicePort(){
                                Name = "redis",
                                Protocol = "TCP",
                                Port = Constants.Ports.Redis,
                                TargetPort = Constants.Ports.Redis
                            }
                        ],

                    };

                    svc.Spec.Selector = new Dictionary<string, string>();
                    svc.Spec.Selector.Add(Constants.Labels.PodName, podName);

                    svc.Metadata.SetOwnerReference(new V1OwnerReference()
                    {
                        ApiVersion = "garnet.k8soperator.io/v1alpha1",
                        Kind       = "GarnetCluster",
                        Name       = pod.GetLabel(Constants.Labels.ClusterName),
                        Uid        = pod.GetLabel(Constants.Labels.ClusterId),
                    });

                    npSvc = await k8s.CoreV1.CreateNamespacedServiceAsync(svc, @namespace, cancellationToken: cancellationToken);
                }
                clusterHost = "10.100.42.100";
                clusterPort = npSvc.Spec.Ports.First().NodePort.Value;
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
        public async Task<List<Shard>> GetShardsAsync(GarnetNode node, CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            var result = await ExecuteRedisCommandAsync(node, true, cancellationToken, "cluster", "shards");

            return JsonSerializer.Deserialize<ShardList>(result).Shards;

        }

        public async Task ForgetNodeAsync(GarnetNode node, IEnumerable<GarnetNode> clusterNodes, CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            if (node.Id.IsNullOrEmpty())
            {
                var client = await CreateClientAsync(node);

                node.Id = await client.MyIdAsync(cancellationToken);

                if (node.Id.IsNullOrEmpty())
                {
                    return;
                }
            }

            foreach (var clusterNode in clusterNodes.Where(p => p.PodUid != node.PodUid && p.PrimaryId != node.Id))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var client = await CreateClientAsync(clusterNode);

                await client.ForgetAsync(node.Id, cancellationToken);
            }
        }

        public async Task ForgetNodeAsync(string id, IEnumerable<GarnetNode> clusterNodes, CancellationToken cancellationToken = default)
        {
            await SyncContext.Clear;

            if (id.IsNullOrEmpty())
            {
                return;
            }

            logger?.LogInformationEx(() => $"Forgetting node: {id}");

            foreach (var clusterNode in clusterNodes.Where(p => p.Id != id))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var client = await CreateClientAsync(clusterNode);

                await client.ForgetAsync(id, cancellationToken);
            }
        }

        private static string CreateKey(params object[] args)
        {
            return string.Join("_", args.Select(x => x.ToString()));
        }
    }
}
