using System;
using System.Collections.Generic;

using k8s.Models;

namespace GarnetOperator.Models
{
    public class V1alpha1GarnetClusterStatus
    {
        public Cluster Cluster { get; set; }

        public List<V1Condition> Conditions { get; set; } = new List<V1Condition>();

        public DateTime StartTime { get; set; } = DateTime.UtcNow;
    }
}