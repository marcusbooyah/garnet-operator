using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using FluentAssertions;

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

        [Fact]
        public void GetSlotCount()
        {
            var node = new GarnetNode();

            node.Slots = new List<int> { 1, 3, 5, 5, 7, 9 };

            node.NumSlots().Should().Be(7);
        }

        [Fact]
        public void GetSlots()
        {
            var node = new GarnetNode();

            node.Slots = new List<int> { 1, 3, 5, 5, 7, 9 };

            node.GetSlots().Count.Should().Be(7);
            node.GetSlots().SequenceEqual(new List<int>() { 1, 2, 3, 5, 7, 8, 9 } ).Should().BeTrue();
        }

        [Fact]
        public void CalculateSlots()
        {
            var nodes = new List<GarnetNode>()
            {
                new GarnetNode(),
                new GarnetNode(),
                new GarnetNode(),
            };
            var numSlices    = nodes.Count;
            var slotsPerNode = Constants.Redis.Slots / numSlices;

            var sb = new StringBuilder();

            for (int i = 0; i < numSlices; i++)
            {
                var start = (i) * slotsPerNode;
                var end   = (1 + i) * slotsPerNode;

                if (i > 0)
                {
                    start++;
                }

                if (i == numSlices - 1)
                {
                    end = Constants.Redis.Slots - 1;
                }

                nodes[i].Slots = new List<int> { start, end };

                sb.AppendLine($"{start} {end}");
            }

            var slots = sb.ToString();
            Assert.True(true);
        }

        [Fact]
        public void GetSlotRanges()
        {
            //var migration = new SlotMigration()
            //{
            //    FromId = "foo",
            //    ToId   = "bar",
            //    Slots = new HashSet<int>()
            //    {
            //        1,2,3,4,5,6,7,15,16,17,18,50,51,52,53,100
            //    }
            //};

            //var ranges = migration.GetSlotRanges();
        }
    }
}
