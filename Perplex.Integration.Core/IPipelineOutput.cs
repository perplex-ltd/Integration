using System;
using System.Collections.Generic;

namespace Perplex.Integration.Core
{

    public interface IPipelineOutput
    {
        void AddRow(Row row);
        void AddRows(IEnumerable<Row> rows);
    }
}