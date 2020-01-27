using Serilog;
using System;
using System.Collections.Generic;

namespace Perplex.Integration.Core.Internal
{

    /// <summary>
    /// The Pipeline Buffer acts as a FIFO queue connecting two DataFlowComponents.
    /// </summary>
    internal class PipelineBuffer : IPipelineInput, IPipelineOutput
    {

        public PipelineBuffer()
        {
            Q = new Queue<Row>();
        }

        protected Queue<Row> Q { get; private set; }

        public void AddRow(Row row)
        {
            Log.Verbose("Adding {row}", row);
            Q.Enqueue(row);
        }

        public void AddRows(IEnumerable<Row> rows)
        {
            if (rows == null) throw new ArgumentNullException(nameof(rows));
            foreach (var row in rows)
            {
                Q.Enqueue(row);
            }
        }

        public Row RemoveRow()
        {
            if (Q.Count == 0) throw new NoMoreRowsException();
            var row = Q.Dequeue();
            Log.Verbose("Removing {row}", row);
            return row;
        }

        public bool HasRowsAvailable
        {
            get { return Q.Count > 0; }
        }

        public long Count => Q.Count;
    }
}