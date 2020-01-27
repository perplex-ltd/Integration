using Perplex.Integration.Core.Configuration;
using Serilog;
using Serilog.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    [Step]
    public class RowLogger : DataProcessStep
    {
        private int recordCounter;

        [Property]
        public LogEventLevel LogLevel { get; set; }

        public RowLogger()
        {
            LogLevel = LogEventLevel.Information;
        }

        public override void Initialise()
        {
            base.Initialise();
            recordCounter = 0;
        }

        public override void Execute()
        {

            while (Input.HasRowsAvailable)
            {
                recordCounter++;
                var row = Input.RemoveRow();
                Log.Write(LogLevel, "{recordCounter} {row}", recordCounter, row);
            }
        }
    }
}
