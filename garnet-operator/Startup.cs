using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Neon.Common;
using Neon.Diagnostics;
using Neon.K8s;
using Neon.K8s.PortForward;
using Neon.Operator;

using Prometheus;

using System;

namespace GarnetOperator
{
    public class OperatorStartup
    {
        public IConfiguration Configuration { get; }

        public OperatorStartup(IConfiguration configuration)
        {
            this.Configuration = configuration;
        }

        public void ConfigureServices(IServiceCollection services)
        {
            var loggerFactory = LoggerFactory.Create(options =>
            {
                if (NeonHelper.IsDevWorkstation)
                {
                    options.SetMinimumLevel(LogLevel.Debug);
                }

                options.ClearProviders();
                options.AddJsonConsole();
            });

            var logger = loggerFactory.CreateLogger<OperatorStartup>();

            services.AddSingleton<ILoggerFactory>(loggerFactory);
            services.AddLogging();
            services.AddKubernetesOperator();

            if (NeonHelper.IsDevWorkstation)
            {
                services.AddSingleton<PortForwardManager>((ctx) =>
                {
                    var loggerFactory = ctx.GetService<ILoggerFactory>();
                    var k8s = KubeHelper.GetKubernetesClient();
                    return new PortForwardManager(k8s, loggerFactory);
                });
            }

            var metricsPort = 9762;

            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("METRICS_PORT")))
            {
                int.TryParse(Environment.GetEnvironmentVariable("METRICS_PORT"), out metricsPort);
            }

            if (!NeonHelper.IsDevWorkstation)
            {
                logger?.LogInformationEx(() => $"Configuring metrics port: {metricsPort}");

                services.AddMetricServer(options =>
                {
                    options.Port = (ushort)metricsPort;
                });
            }
        }

        public void Configure(IApplicationBuilder app)
        {
            app.UseKubernetesOperator();
        }
    }
}
