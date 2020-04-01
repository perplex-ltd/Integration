using Perplex.Integration.Core.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    [Step]
    public class LimitRecords : DataProcessStep
    {

        [IntegerProperty(Required = true, Description = "The maximum number of records passed through this step.")]
        public int Top { get; set; }

        private int rowCounter;

        public override void Initialise()
        {
            base.Initialise();
            rowCounter = 0;
        }

        public override void Execute()
        {
            while (Input.HasRowsAvailable)
            {
                var row = Input.RemoveRow();
                rowCounter++;
                if (rowCounter <= Top)
                {
                    Output.AddRow(row);
                }
            }
        }

    }
}
