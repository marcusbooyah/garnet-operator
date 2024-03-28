using System.ComponentModel;

namespace GarnetOperator.Models
{
    public class RollingUpdate : Scaling
    {
        [DefaultValue(true)]
        public bool KeyMigration { get; set; } = true;

        [DefaultValue(0)]
        public int WarmingDelayMillis { get; set; } = 0;
    }
}