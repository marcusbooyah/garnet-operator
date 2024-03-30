using System;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Neon.Common;
using Neon.Operator;

using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace GarnetOperator
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            var listenPort = 5000;

            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("LISTEN_PORT")))
            {
                int.TryParse(Environment.GetEnvironmentVariable("LISTEN_PORT"), out listenPort);
            }

            var host = KubernetesOperatorHost
               .CreateDefaultBuilder()
               .ConfigureOperator(settings =>
               {
                   settings.AssemblyScanningEnabled  = false;
                   settings.Name                     = Constants.OperatorName;
                   settings.Port                     = listenPort;
                   if (NeonHelper.IsDevWorkstation)
                   {
                       settings.PodNamespace             = "garnet";
                       settings.UserImpersonationEnabled = false;
                   }
               })
               .UseStartup<Startup>();

            var tracingOtlpEndpoint = Environment.GetEnvironmentVariable("TRACING_OTLP_ENDPOINT");

            if (!string.IsNullOrEmpty(tracingOtlpEndpoint))
            {
                host.Services.AddOpenTelemetry()
                    .ConfigureResource(resource => resource
                    .AddService(serviceName: TraceContext.ActivitySourceName))
                    .WithTracing(tracing =>
                    {
                        tracing.AddAspNetCoreInstrumentation();
                        tracing.AddKubernetesOperatorInstrumentation();
                        tracing.AddSource(TraceContext.ActivitySourceName);
                        tracing.AddOtlpExporter(otlpOptions =>
                        {
                            otlpOptions.Endpoint = new Uri(tracingOtlpEndpoint);
                        });
                    });
            }

            await host.Build().RunAsync();
        }
    }
}
