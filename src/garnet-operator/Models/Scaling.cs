using System.ComponentModel;

namespace GarnetOperator.Models
{

    /// <summary>
    /// Represents the scaling configuration.
    /// </summary>
    public class Scaling
    {
        /// <summary>
        /// Gets or sets the idle timeout in milliseconds.
        /// </summary>
        [DefaultValue(30000)]
        public int IdleTimeoutMillis { get; set; } = 30000;

        /// <summary>
        /// Gets or sets the key batch size.
        /// </summary>
        [DefaultValue(10000)]
        public int KeyBatchSize { get; set; } = 10000;

        /// <summary>
        /// Gets or sets the slot batch size.
        /// </summary>
        [DefaultValue(16)]
        public int SlotBatchSize { get; set; } = 16;
    }
}