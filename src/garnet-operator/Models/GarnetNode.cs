using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;

using k8s.Models;

namespace GarnetOperator.Models
{

    /// <summary>
    /// Represents a Garnet node.
    /// </summary>
    public class GarnetNode
    {
        /// <summary>
        /// Gets or sets the ID of the node.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets the role of the node.
        /// </summary>
        [System.Text.Json.Serialization.JsonConverter(typeof(System.Text.Json.Serialization.JsonStringEnumMemberConverter))]
        [DefaultValue(GarnetRole.None)]
        public GarnetRole Role { get; set; } = GarnetRole.None;

        /// <summary>
        /// Gets or sets the zone of the node.
        /// </summary>
        public string Zone { get; set; }

        /// <summary>
        /// Gets or sets the address of the node.
        /// </summary>
        public string Address { get; set; }

        /// <summary>
        /// Gets or sets the pod IP of the node.
        /// </summary>
        public string PodIp { get; set; }

        /// <summary>
        /// Gets or sets the port of the node.
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// Gets or sets the slots of the node.
        /// </summary>
        public List<int> Slots { get; set; }

        /// <summary>
        /// Gets or sets the primary ID of the node.
        /// </summary>
        public string PrimaryId { get; set; }

        /// <summary>
        /// Gets or sets the pod name of the node.
        /// </summary>
        public string PodName { get; set; }

        /// <summary>
        /// Gets or sets the pod UID of the node.
        /// </summary>
        public string PodUid { get; set; }

        /// <summary>
        /// Gets or sets the node name of the node.
        /// </summary>
        public string NodeName { get; set; }

        /// <summary>
        /// Gets or sets the namespace of the node.
        /// </summary>
        public string Namespace { get; set; }

        /// <summary>
        /// Gets or sets the config epoch of the node.
        /// </summary>
        public int ConfigEpoch { get; set; }

        /// <summary>
        /// Calculates the total number of slots in the node.
        /// </summary>
        /// <returns>The total number of slots in the node.</returns>
        public int NumSlots()
        {
            var result = 0;

            if (Slots == null)
            {
                return result;
            }

            for (int i = 0; i < Slots.Count; i += 2)
            {
                result += Slots[i + 1] - Slots[i] + 1;
            }

            return result;
        }

        /// <summary>
        /// Gets the list of all slots in the node.
        /// </summary>
        /// <returns>The list of all slots in the node.</returns>
        public List<int> GetSlots()
        {
            var result = new List<int>();

            if (Slots == null)
            {
                return result;
            }

            for (int i = 0; i < Slots.Count; i += 2)
            {
                result.AddRange(Enumerable.Range(Slots[i], Slots[i + 1] - Slots[i] + 1));
            }

            return result;
        }
    }

}
