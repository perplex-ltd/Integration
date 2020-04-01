using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{

    [Step]
    public class FindDuplicates : DataProcessStep
    {

        [Property(Description = "If true, any null key comparision to any other value will yield true.")]
        public bool TreatMissingValuesAsIdentical { get; set; } = false;

        [Property("Criteria", Required = true, Description = "The name of the fields to check for duplicates.")]
        public IList<string> Criteria { get; } = new List<string>();

        [Property("Key", Required = true, Description = "The name of the id field used to create the output.")]
        public IList<string> Keys { get; } = new List<string>();

        Row lastRow;

        public override void Validate()
        {
            base.Validate();
            if (Keys.Count == 0) throw new InvalidConfigurationException("No Keys specified.");
            if (Criteria.Count == 0) throw new InvalidConfigurationException("No Criteria specified.");
        }

        public override void Initialise()
        {
            base.Initialise();
            lastRow = null;
        }

        public override void Execute()
        {
            var count = 0;
            while (Input.HasRowsAvailable)
            {
                var row = Input.RemoveRow();
                if (lastRow != null)
                {
                    var (isDuplicate, master, subordinate) = DetectDuplicates(lastRow, row);
                    if (isDuplicate)
                    {
                        count++;
                        Log.Verbose("Found a duplicate\n{master}\n{subordinate}", master, subordinate);
                        var duplicateRow = new Row();
                        AddKeys(duplicateRow, master, "master$");
                        AddKeys(duplicateRow, subordinate, "subordinate$");
                        Output.AddRow(duplicateRow);
                        lastRow = master;
                    }
                    else
                    {
                        lastRow = row;
                    }
                }
                else
                {
                    lastRow = row;
                }
            }
            Log.Information("Found {count} duplicates.", count);
        }

        /// <summary>
        /// Copy all key attribute from <paramref name="original"/> to <paramref name="row"/>, 
        /// with the <paramref name="prefix"/> added to each key name.
        /// </summary>
        /// <param name="row"></param>
        /// <param name="original"></param>
        /// <param name="prefix"></param>
        private void AddKeys(Row row, Row original, string prefix)
        {
            foreach (var key in Keys)
            {
                row[prefix + key] = original[key];
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="aRow"></param>
        /// <param name="anotherRow"></param>
        /// <returns>A Tuple containg a variable indictating where the rows are duplicate, 
        /// the master row and the subordinate row</returns>
        private (bool, Row, Row) DetectDuplicates(Row aRow, Row anotherRow)
        {
            bool identical = Criteria.Count > 0;
            var nonNullFields1 = 0;
            var nonNullFields2 = 0;
            foreach (var field in Criteria)
            {
                var k1 = GetValue(field, aRow);
                var k2 = GetValue(field, anotherRow);
                if (!string.IsNullOrEmpty(k1)) nonNullFields1++;
                if (!string.IsNullOrEmpty(k2)) nonNullFields2++;
                if (TreatMissingValuesAsIdentical)
                {
                    identical &= (k1 == null) || (k2 == null) || String.CompareOrdinal(k1, k2) == 0;
                }
                else
                {
                    identical &= (k1 == k2);
                }
                if (!identical) break;
            }
            var master = aRow;
            var subordinate = anotherRow;
            if (identical && nonNullFields2 > nonNullFields1)
            {
                master = anotherRow;
                subordinate = aRow;
            }
            
            return (identical, master, subordinate);
        }

        private static string GetValue(string key, Row aRow)
        {
            var value = (aRow.ContainsKey(key)) ? aRow[key] : null;
            return value?.ToString();
        }
    }
}
