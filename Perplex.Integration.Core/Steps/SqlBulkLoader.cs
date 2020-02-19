using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace Perplex.Integration.Core.Steps
{
    [Configuration.Step(Description = "Loads all rows into a Sql table.")]
    public class SqlBulkLoader : AbstractSqlStep, IDataSink
    {

        public IPipelineInput Input { get; set; }


        [IntegerProperty(MinValue = 0, MaxValue = 100000)]
        public int BatchSize { get; set; } = 1000;
        [IntegerProperty(MinValue = 0, MaxValue = 100000, Description = "The timeout for the bulk load operation in seconds.")]
        public int Timeout { get; set; } = 600;

        [Property(Required = true, Description = "The name of the table to load the rows into.")]
        public string TableName { get; set; }


        private List<string> destinationColumns;

        public override void Initialise()
        {
            base.Initialise();
            destinationColumns = new List<string>();
            using SqlCommand cmd = DbConnection.CreateCommand();
            cmd.CommandText = $"select [name] from sys.columns where object_id = object_id('{TableName}')";
            using  var rs = cmd.ExecuteReader();
            while (rs.Read())
            {
                destinationColumns.Add(rs.GetString(0)); 
            }
        }

        public override void Execute()
        {
            using var dt = new DataTable();
            while (Input.HasRowsAvailable)
            {
                var row = Input.RemoveRow();
                // Ensure columns
                foreach (var column in row.Columns)
                {
                    if (!dt.Columns.Contains(column) && row[column] != null)
                    {
                        dt.Columns.Add(column, row[column].GetType());
                    }
                }
                // Convert row to DataRow
                var tableRow = dt.NewRow();
                foreach (var column in row.Columns)
                {
                    tableRow[column] = row[column];
                }
                dt.Rows.Add(tableRow);
            }
            using var bulkCopy = new SqlBulkCopy(DbConnection)
            {
                DestinationTableName = TableName,
                BatchSize = BatchSize,
                BulkCopyTimeout = Timeout
            };
            // add column mappings
            int srcPos = 0;
            foreach (DataColumn column in dt.Columns)
            {
                int destPos = destinationColumns.IndexOf(column.ColumnName);
                if (destPos < 0) throw new StepException(Id, $"Column {column.ColumnName} does not exist in target table {TableName}.");
                bulkCopy.ColumnMappings.Add(srcPos, destPos);
                srcPos++;
            }
            Log.Debug("Writing {rowCount} rows to {DestinationTableName}", dt.Rows.Count, bulkCopy.DestinationTableName);
            bulkCopy.WriteToServer(dt);
        }



    }
}
