using System.Collections.Generic;
using System.Linq;

using k8s.Models;

namespace GarnetOperator.Models
{
    public class GarnetNode
    {
        public string Id { get; set; }

        [System.Text.Json.Serialization.JsonConverter(typeof(System.Text.Json.Serialization.JsonStringEnumMemberConverter))]
        public GarnetRole Role { get; set; }

        public string Zone { get; set; }

        public string Address { get; set; }

        public int Port { get; set; }

        public List<int> Slots { get; set; }

        public string PrimaryId { get; set; }

        public string PodName { get; set; }
        public string PodUid { get; set; }
        public string NodeName { get; set; }
        public string Namespace { get; set; }

        public int NumSlots()
        {
            var result = 0;

            for (int i = 0; i < Slots.Count; i += 2)
            {
                result += Slots[i + 1] - Slots[i] + 1;
            }

            return result;
        }

        public List<int> GetSlots()
        {
            var result = new List<int>();

            for (int i = 0; i < Slots.Count; i += 2)
            {
                result.AddRange(Enumerable.Range(Slots[i], Slots[i + 1] - Slots[i] + 1));
            }

            return result;
        }
    }

}
