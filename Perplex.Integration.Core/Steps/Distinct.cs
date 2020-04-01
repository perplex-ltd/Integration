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
    public class Distinct : DataProcessStep
    {

        [Property(Required=true)]
        public string KeyField { get; set; }

        readonly ICollection<object> recordsAlreadyAdded = new SortedSet<object>();

        public override void Initialise()
        {
            base.Initialise();
            recordsAlreadyAdded.Clear();
        }

        public override void Execute()
        {
            int counter = 0;
            while (Input.HasRowsAvailable)
            {
                var row = Input.RemoveRow();
                var key = row[KeyField];
                if (key is null) throw new StepException("Key must not be null");
                if (!recordsAlreadyAdded.Contains(key))
                {
                    recordsAlreadyAdded.Add(key);
                    Output.AddRow(row);
                    counter++;
                }
            }
        }
    }
}
