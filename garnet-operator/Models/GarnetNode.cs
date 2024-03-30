using System.Collections.Generic;

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

        public List<string> Slots { get; set; }

        public string PrimaryId { get; set; }

        public string PodName { get; set; }
        public string PodUid { get; set; }
        public string NodeName { get; set; }
        public string Namespace { get; set; }
    }
}
