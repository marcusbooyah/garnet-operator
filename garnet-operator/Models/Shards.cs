using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace GarnetOperator.Models
{
    [System.Text.Json.Serialization.JsonConverter(typeof(ShardConverter))]
    public class ShardList
    {
        public List<Shard> Shards { get; set; }
    }

    public class Shard
    {
        public List<int> Slots { get; set; }
        public List<Node> Nodes { get; set; }
    }

    public class Node
    {
        public string Id { get; set; }
        public int Port { get; set; }
        public string Address { get; set; }
        public GarnetRole Role { get; set; }
        public int ReplicationOffset { get; set; }
        public string Health { get; set; }
    }


    public sealed class ShardConverter : JsonConverter<ShardList>
    {
        public override ShardList Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            reader.Read();
            var shards = new ShardList();
            shards.Shards = new List<Shard>();

            while (reader.TokenType == JsonTokenType.StartArray)
            {
                var tokenType = reader.TokenType;

                var shard = new Shard();
                shard.Slots = new List<int>();

                while (reader.TokenType != JsonTokenType.StartArray)
                {
                    reader.Read();
                }

                while (reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.Number)
                    {
                        shard.Slots.Add(reader.GetInt32());
                    }
                    reader.Read();
                }
                reader.Read();
                reader.Read();
                reader.Read();

                var nodes = new List<Node>();

                reader.Read();
                while (reader.TokenType != JsonTokenType.EndArray)
                {
                    var node = new Node();
                    while (reader.TokenType != JsonTokenType.String)
                    {
                        reader.Read();
                    }
                    reader.Read();
                    node.Id = reader.GetString();
                    reader.Read();
                    reader.Read();
                    node.Port = reader.GetInt32();
                    reader.Read();
                    reader.Read();
                    node.Address = reader.GetString();
                    reader.Read();
                    reader.Read();
                    node.Role = RoleFromString(reader.GetString());
                    reader.Read();
                    reader.Read();
                    node.ReplicationOffset = reader.GetInt32();
                    reader.Read();
                    reader.Read();
                    node.Health = reader.GetString();
                    reader.Read();
                    reader.Read();

                    nodes.Add(node);

                    if (reader.TokenType == JsonTokenType.EndArray)
                    {
                        shard.Nodes = nodes;
                        shards.Shards.Add(shard);
                        reader.Read();
                    }
                }

                reader.Read();
            }
            return shards;
        }

        public override void Write(Utf8JsonWriter writer, ShardList value, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }

        public static GarnetRole RoleFromString(string role)
        {
            switch (role)
            {
                case "PRIMARY":

                    return GarnetRole.Primary;

                case "REPLICA":

                    return GarnetRole.Replica;

                default:

                    throw new ArgumentException("Unknown role");
            }
        }
    }
}
