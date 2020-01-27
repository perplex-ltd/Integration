using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Internal
{
    class NullPipelineOutput : IPipelineOutput
    {

        public void AddRow(Row row)
        {           
        }

        public void AddRows(IEnumerable<Row> rows)
        {
        }

    }
}
