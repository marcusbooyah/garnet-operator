using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using FluentAssertions;

using GarnetOperator.Models;

namespace Test_GarnetOperator
{
    public class Test_Json
    {
        [Fact]
        public void DeserializeShards()
        {
            var output = $@"[
  [
    ""slots"",
    [
      0,
      100
    ],
    ""nodes"",
    [
      [
        ""id"",
        ""2fa09a81f6cd700e468b9511c93088b658592f5f"",
        ""port"",
        6379,
        ""address"",
        ""0.0.0.0"",
        ""role"",
        ""PRIMARY"",
        ""replication-offset"",
        64,
        ""health"",
        ""online""
      ],
      [
        ""id"",
        ""959f231ad7d4534177a73e1f5bd32878202b8ca5"",
        ""port"",
        6379,
        ""address"",
        ""0.0.0.0"",
        ""role"",
        ""REPLICA"",
        ""replication-offset"",
        0,
        ""health"",
        ""online""
      ],
      [
        ""id"",
        ""75a39e1bd92552897d9a88ce0b6849e18b3ee660"",
        ""port"",
        6379,
        ""address"",
        ""0.0.0.0"",
        ""role"",
        ""REPLICA"",
        ""replication-offset"",
        0,
        ""health"",
        ""online""
      ]
    ]
  ],
  [
    ""slots"",
    [
      
    ],
    ""nodes"",
    [
      [
        ""id"",
        ""446a994bf121ff855a39805d5ac2e0fa051acb80"",
        ""port"",
        6379,
        ""address"",
        ""0.0.0.0"",
        ""role"",
        ""PRIMARY"",
        ""replication-offset"",
        0,
        ""health"",
        ""online""
      ],
      [
        ""id"",
        ""15983b0ac52c6949989c91f126bc249d2bb3065e"",
        ""port"",
        6379,
        ""address"",
        ""0.0.0.0"",
        ""role"",
        ""REPLICA"",
        ""replication-offset"",
        0,
        ""health"",
        ""online""
      ],
      [
        ""id"",
        ""18dfe59146a0149859bd882ab2f0fe9edc6311c3"",
        ""port"",
        6379,
        ""address"",
        ""0.0.0.0"",
        ""role"",
        ""REPLICA"",
        ""replication-offset"",
        0,
        ""health"",
        ""online""
      ]
    ]
  ]
]";

            var options = new JsonSerializerOptions();

            var shardList = JsonSerializer.Deserialize<ShardList>(output);

            shardList.Shards.Should().HaveCount(2);
            shardList.Shards.First().Nodes.Should().HaveCount(3);
            shardList.Shards.First().Slots.First().Should().Be(0);
            shardList.Shards.First().Slots.Last().Should().Be(100);

        }
    }
}
