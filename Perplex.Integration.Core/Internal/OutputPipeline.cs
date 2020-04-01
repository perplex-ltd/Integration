using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Internal
{
    internal class OutputPipeline : IPipelineOutput
    {
        public void AddRow(Row row)
        {
            foreach (var buffer in buffers)
            {
                buffer.AddRow(new Row(row));
            }
        }

        public void AddRows(IEnumerable<Row> rows)
        {
            foreach (var r in rows)
            {
                AddRow(r);
            }
        }

        readonly IList<PipelineBuffer> buffers = new List<PipelineBuffer>();

        public IPipelineInput CreateInputPipeline()
        {
            var inputPipeline = new PipelineBuffer();
            buffers.Add(inputPipeline);
            return inputPipeline;
        }


    }
}
