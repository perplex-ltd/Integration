using Perplex.Integration.Core.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{

    [Step]
    public class TransformFields : DataProcessStep
    {

        public TransformFields() { }

        [Property("Rename")]
        public IList<FieldRenameMapping> RenameMappings { get; } = new List<FieldRenameMapping>();

        [Property("Remove")]
        public IList<string> Remove { get; } = new List<string>();

        [Property("Add")]
        public IList<FieldAddMapping> Add { get; } = new List<FieldAddMapping>();




        public override void Execute()
        {
            while (Input.HasRowsAvailable)
            {
                var inRow = Input.RemoveRow();
                var outRow = new Row();
                foreach (var column in inRow.Keys)
                {
                    var rename = RenameMappings.FirstOrDefault(m => string.Equals(m.From, column, StringComparison.OrdinalIgnoreCase));
                    if (rename != null)
                    {
                        outRow[rename.To] = inRow[rename.From];
                    }
                    else if (!Remove.Contains(column))
                    {
                        outRow[column] = inRow[column];
                    }
                }
                foreach (var mapping in Add)
                {
                    outRow[mapping.Field] = mapping.Value;
                }
                Output.AddRow(outRow);
            }
        }
    }
}
