using Perplex.Integration.Core.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{

     [Configuration.Step("Field Renamer")]
    public class RenameFieldNames : DataProcessStep
    {

        public RenameFieldNames()
        {
            Mappings = new List<Mapping>();
        }

        [Property("Rename")]
        public IList<Mapping> Mappings { get; }


        public override void Execute()
        {
            while (Input.HasRowsAvailable)
            {
                var row = Input.RemoveRow();
                foreach (var mapping in Mappings)
                {
                    if (row.ContainsKey(mapping.From))
                    {
                        row[mapping.To] = row[mapping.From];
                        row.Remove(mapping.From);
                    }
                }
                Output.AddRow(row);
            }
        }
    }
}
