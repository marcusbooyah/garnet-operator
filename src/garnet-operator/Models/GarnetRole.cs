using System.Runtime.Serialization;

namespace GarnetOperator.Models
{

    [System.Text.Json.Serialization.JsonConverter(typeof(System.Text.Json.Serialization.JsonStringEnumMemberConverter))]
    public enum GarnetRole
    {
        /// <summary>
        /// Represents no role.
        /// </summary>
        [EnumMember(Value = "none")]
        None = 0,

        /// <summary>
        /// Represents the primary role.
        /// </summary>
        [EnumMember(Value = "primary")]
        Primary,

        /// <summary>
        /// Represents the replica role.
        /// </summary>
        [EnumMember(Value = "replica")]
        Replica,

        /// <summary>
        /// Represents the handshake role.
        /// </summary>
        [EnumMember(Value = "handshake")]
        Handshake,

        /// <summary>
        /// Represents the leaving role.
        /// </summary>
        [EnumMember(Value = "leaving")]
        Leaving,

        /// <summary>
        /// Represents the promoting role.
        /// </summary>
        [EnumMember(Value = "promoting")]
        Promoting,

        /// <summary>
        /// Represents the demoting role.
        /// </summary>
        [EnumMember(Value = "demoting")]
        Demoting
    }
}
