using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GarnetOperator.Models
{
    public class SlotMigration
    {
        public string FromId { get; set; }
        public string ToId { get; set; }

        public HashSet<int> Slots { get; set; }

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
