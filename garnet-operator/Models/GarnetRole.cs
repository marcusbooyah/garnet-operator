using System.Runtime.Serialization;

namespace GarnetOperator.Models
{
    [System.Text.Json.Serialization.JsonConverter(typeof(System.Text.Json.Serialization.JsonStringEnumMemberConverter))]
    public enum GarnetRole
    {
        [EnumMember(Value = "primary")]
        Primary = 0,

        [EnumMember(Value = "replica")]
        Replica,

        [EnumMember(Value = "handshake")]
        Handshake,

        [EnumMember(Value = "none")]
        None,

        [EnumMember(Value = "leaving")]
        Leaving,

        [EnumMember(Value = "promoting")]
        Promoting
    }
}
