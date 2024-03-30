using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using GarnetOperator;
using GarnetOperator.Models;

using k8s;
using k8s.Models;

using Neon.K8s.Core;
using Neon.Operator.Xunit;

namespace Test_GarnetOperator
{
    public class Test_ScalingOperations : IClassFixture<TestOperatorFixture>
    {
        private TestOperatorFixture fixture;
        public Test_ScalingOperations(TestOperatorFixture fixture)
        {
            this.fixture = fixture;
            fixture.Operator.AddController<GarnetClusterController>();
            fixture.RegisterType<V1ConfigMap>();
            fixture.RegisterType<V1PodDisruptionBudget>();
            fixture.RegisterType<V1alpha1GarnetCluster>();
            fixture.RegisterType<V1Service>();
            fixture.RegisterType<V1Pod>();
            fixture.Start();
        }

        [Fact]
        public async Task CreateGarnetClusterAsync()
        {

            fixture.ClearResources();

            var controller = fixture.Operator.GetController<GarnetClusterController>();

            var cluster = new V1alpha1GarnetCluster().Initialize();
            cluster.Metadata.Name = "test-cluster";
            cluster.Metadata.NamespaceProperty = "default";
            cluster.Spec = new V1alpha1GarnetClusterSpec();
            cluster.Spec.NumberOfPrimaries = 3;
            cluster.Spec.ReplicationFactor = 2;

            var s = KubernetesHelper.YamlSerialize(cluster);

            fixture.AddResource(cluster);

            await controller.ReconcileAsync(cluster);
        }
    }
}
