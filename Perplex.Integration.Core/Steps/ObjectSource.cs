using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    [Step()]
    public class ObjectSource : DataSourceStep
    {

        [Property("JsonObject")]
        public IList<string> JsonObjects { get; private set; }

        public ObjectSource()
        {
            JsonObjects = new List<string>();
        }

        /// <summary>
        /// Adds all JsonObjects to the Output.
        /// </summary>
        public override void Execute()
        {
            foreach (var row in JsonObjects.Select(o => Row.FromJson(o)))
            {
                Output.AddRow(row);
            }
        }

    }
}
