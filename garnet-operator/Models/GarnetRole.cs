using System.Runtime.Serialization;

namespace GarnetOperator.Models
{
    [System.Text.Json.Serialization.JsonConverter(typeof(System.Text.Json.Serialization.JsonStringEnumMemberConverter))]
    public enum GarnetRole
    {
        [EnumMember(Value = "none")]
        None = 0,

        [EnumMember(Value = "primary")]
        Primary,

        [EnumMember(Value = "replica")]
        Replica,

        [EnumMember(Value = "handshake")]
        Handshake,

        [EnumMember(Value = "leaving")]
        Leaving,

        [EnumMember(Value = "promoting")]
        Promoting,

        [EnumMember(Value = "demoting")]
        Demoting
    }
}
