using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    [Step(Description = "Executes a SQL query for each record, passing the record's fields as parameters to the query.")]
    public class SqlSink : AbstractSqlStep, IDataSink
    {
        public IPipelineInput Input { get; set; }
        [Property(Required = true, Description = "A query that is used to insert or update rows.")]
        public string Query { get; set; }
        [Property()]
        public bool ContinueOnError { get; set; } = false;

        public override void Execute()
        {
            using var cmd = DbConnection.CreateCommand();
            cmd.CommandText = Query;
            cmd.CommandTimeout = CommandTimeout;
            int counter = 0;
            while (Input.HasRowsAvailable)
            {
                var row = Input.RemoveRow();
                // clear parameter values...
                foreach (SqlParameter p in cmd.Parameters)
                {
                    p.Value = null;
                }
                // create parameters
                foreach (var column in row.Columns)
                {
                    var parameter = "@" + column;
                    if (!cmd.Parameters.Contains(parameter))
                    {
                        cmd.Parameters.AddWithValue(parameter, row[column]);
                    }
                    else
                    {
                        cmd.Parameters[parameter].Value = row[column];
                    }
                }
                try
                {
                    int affectedRows = cmd.ExecuteNonQuery();
                    Log.Verbose("Command {cmd} affected {affectedRows} rows.", cmd, affectedRows);
                    if (++counter % 10000 == 0)
                    {
                        Log.Debug("Pushed {counter} rows", counter);
                    }
                }
                catch (Exception ex)
                {
                    Log.Error("'{ex.Message}' for row {row}", ex.Message, row);
                    if (!ContinueOnError)
                    {
                        throw new StepException("Failed to run query.");
                    }
                }
            }
            Log.Information("Pushed a total of {counter} rows", counter);
        }
    }
}
