using GarnetOperator;
using FluentAssertions;
using System.Collections.Generic;
using k8s.Models;

namespace Test_GarnetOperator
{
    public class Test_Extensions
    {
        [Fact]
        public void TestDictToLabelSelector()
        {
            var labels = new Dictionary<string, string>();

            labels.ToLabelSelector().Should().BeEmpty();

            labels.Add("foo", "bar");
            labels.ToLabelSelector().Should().Be("foo=bar");

            labels.Add("123", "456");
            labels.ToLabelSelector().Should().Be("foo=bar,123=456");

            labels.Add("bar", "baz");

            labels.ToLabelSelector().Should().Be("foo=bar,123=456,bar=baz");
        }

        [Fact]
        public void TestJsonEquals()
        {
            var serviceA = new V1ServiceSpec()
            {
                SessionAffinity = "foo",
                ClusterIP = "127.0.0.1",
                ExternalName = "foo",
                ClusterIPs = ["127.0.0.1"],
            };

            var serviceB = new V1ServiceSpec()
            {
                SessionAffinity = "foo",
                ClusterIP = "127.0.0.1",
                ExternalName = "foo",
                ClusterIPs = ["127.0.0.1"],
            };

            serviceA.JsonEquals(serviceB).Should().BeTrue();

            serviceB.ClusterIPs.Add("0.0.0.0");

            serviceA.JsonEquals(serviceB).Should().BeFalse();
        }
    }
}