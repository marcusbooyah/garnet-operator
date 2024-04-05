using System.ComponentModel;

namespace GarnetOperator.Models
{

    /// <summary>
    /// Represents a rolling update configuration for scaling.
    /// </summary>
    public class RollingUpdate : Scaling
    {
        /// <summary>
        /// Gets or sets a value indicating whether key migration is enabled during the rolling update.
        /// </summary>
        [DefaultValue(true)]
        public bool KeyMigration { get; set; } = true;

        /// <summary>
        /// Gets or sets the warming delay in milliseconds before starting the rolling update.
        /// </summary>
        [DefaultValue(0)]
        public int WarmingDelayMillis { get; set; } = 0;
    }
}