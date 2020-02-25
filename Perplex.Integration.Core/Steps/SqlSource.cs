using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    [Step(Description = "Retrieves records from a SQL database using a query.")]
    public class SqlSource : AbstractSqlStep, IDataSource
    {
        public IPipelineOutput Output { get; set; }
        [Property(Required = true, Description = "A SELECT query that is used to retrieve rows.")]
        public string Query { get; set; }

        public override void Execute()
        {
            using var cmd = DbConnection.CreateCommand();
            cmd.CommandText = Query;
            cmd.CommandTimeout = CommandTimeout;
            using var rs = cmd.ExecuteReader();
            int counter = 0;
            while (rs.Read())
            {
                counter++;
                var row = new Row();
                for (int i = 0; i < rs.VisibleFieldCount; i++)
                {
                    row.Add(rs.GetName(i), rs.GetValue(i));
                }
                Output.AddRow(row);
                if (counter % 5000 == 0)
                {
                    Log.Debug("Got {counter} rows", counter);
                }
            }
            Log.Debug("Got a total of {counter} rows", counter);
        }
    }
}
