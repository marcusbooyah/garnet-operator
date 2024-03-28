using System.ComponentModel;

namespace GarnetOperator.Models
{
    public class Scaling
    {
        [DefaultValue(30000)]
        public int IdleTimeoutMillis { get; set; } = 30000;

        [DefaultValue(10000)]
        public int KeyBatchSize { get; set; } = 10000;

        [DefaultValue(16)]
        public int SlotBatchSize { get; set; } = 16;
    }
}