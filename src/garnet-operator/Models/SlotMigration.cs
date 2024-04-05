using System.Collections.Generic;
using System.Linq;

namespace GarnetOperator.Models
{

    /// <summary>
    /// Represents a slot migration.
    /// </summary>
    public class SlotMigration
    {
        /// <summary>
        /// Gets or sets the ID of the slot migration source.
        /// </summary>
        public string FromId { get; set; }

        /// <summary>
        /// Gets or sets the destination node of the slot migration.
        /// </summary>
        public GarnetNode ToNode { get; set; }

        /// <summary>
        /// Gets or sets the set of slots to be migrated.
        /// </summary>
        public HashSet<int> Slots { get; set; }

        /// <summary>
        /// Gets the list of slot ranges for the migration.
        /// </summary>
        /// <returns>The list of slot ranges.</returns>
        public List<SlotRange> GetSlotRanges()
        {
            var sorted = Slots.Order().ToList();
            int start = sorted.First();

            var result = new List<SlotRange>();
            var range = new SlotRange()
            {
                Min = start,
                Max = start
            };
            for (int i = 0; i < Slots.Count; i++)
            {
                if (sorted[i] > range.Max + 1)
                {
                    result.Add(range);
                    range = new SlotRange()
                    {
                        Min = sorted[i],
                        Max = sorted[i]
                    };
                    continue;
                }

                range.Max = sorted[i];
            }

            result.Add(range);

            return result;
        }
    }
}
