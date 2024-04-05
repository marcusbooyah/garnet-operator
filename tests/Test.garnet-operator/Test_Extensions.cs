using System.Collections.Generic;
using System.Linq;

using FluentAssertions;

using GarnetOperator;
using GarnetOperator.Models;

using k8s.Models;

using Neon.Common;

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

        [Fact]
        public void ParseNodes()
        {
            var resp = $@"dc9e3ce9d4d9a25bf35c6aab646401d59b294f66 10.254.38.113:6379@16379,test-cluster-1f6bb8i0mwdp3 myself,master - 0 0 1 connected 0-5460
6b9e3b0979069ccc410e151edf2fdaeb4bd7584f 10.254.43.34:6379@16379,test-cluster-ac13fpamzew52 master - 0 0 2 connected 5461-10921
c6631cc5a5e55fae2f845f7c3e054552ccdcf637 10.254.97.233:6379@16379,test-cluster-k0igdwjnh6tw1 master - 0 0 3 connected 10922-16383
1de7ec91ea5603e0341518c48d92ab707f9e76a2 10.254.226.153:6379@16379,test-cluster-i7fbrwmsihwc1 slave 6b9e3b0979069ccc410e151edf2fdaeb4bd7584f 0 0 4 connected
ddf92cd8e69df7bdcb2697093d33ea5a13ec9f1f 10.254.133.219:6379@16379,test-cluster-8jsngt0a5toj1 slave dc9e3ce9d4d9a25bf35c6aab646401d59b294f66 0 0 6 connected
e5a04da709ac664d536a105e5487cac1b785a924 10.254.226.176:6379@16379,test-cluster-6xhkzi9imwdp1 slave c6631cc5a5e55fae2f845f7c3e054552ccdcf637 0 0 5 connected";

            var nodes = new List<ClusterNode>();

            foreach (var line in resp.ToLines())
            {
                nodes.Add(ClusterNode.FromRespResponse(line));
            }

            nodes.Should().HaveCount(6);
            nodes.First().Slots.Should().HaveCount(2);
            nodes.First().Slots.First().Should().Be(0);
            nodes.First().Slots[1].Should().Be(5460);
            nodes.Take(3).All(n => n.Flags.Any(f => f.Equals("master"))).Should().BeTrue();
            nodes.Skip(3).All(n => n.Flags.Any(f => f.Equals("slave"))).Should().BeTrue();
        }
    }
}