using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    [Step()]
    public class SqlJsonSink : AbstractSqlStep, IDataSink
    {
        public string BulkLoadTableName { get; set; } = "#TempJsonObjects";

        public IPipelineInput Input { get; set; }

        [IntegerProperty(MinValue = 0, MaxValue = 100000)]
        public int BatchSize { get; set; }
        [IntegerProperty(MinValue = 0, MaxValue = 100000, Description = "The timeout for the bulk copy operation in seconds.")]
        public int Timeout { get; set; }

        [Property(Required = true, Description = "The name of the table to save the objects.")]
        public string TableName { get; set; }
        [Property(Required = true, Description = "The name of the primary key field of the data.")]
        public string PrimaryKeyField { get; set; }


        public SqlJsonSink()
        {
            BatchSize = 1000;
            Timeout = 600;
        }

        /// <summary>
        /// Creates the table specified by TableName if it does not exists yet.
        /// </summary>
        public override void Initialise()
        {
            base.Initialise();
            using SqlCommand cmd = DbConnection.CreateCommand();
            cmd.CommandText = $@"
if not exists(select * from sys.tables where [name] = @TableName)
begin
	create table [{TableName}] (
        Id nvarchar(200) primary key clustered,
		JsonObject nvarchar(max) not null,
        UpdatedOn DateTime not null
	);
end
";
            cmd.Parameters.AddWithValue("@TableName", TableName);
            Log.Verbose("Initialising table {TableName} with {CommandText}", TableName, cmd.CommandText);
            cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Inserts records into a temporary table, then merges them.
        /// </summary>
        public override void Execute()
        {
            // creating temp table
            InsertRecords();
            // Merge temp table 
            Log.Debug("Merging records...");
            MergeRecords();

        }

        public override void Cleanup()
        {
            using var dropTempTableCmd = DbConnection.CreateCommand();
            dropTempTableCmd.CommandText = $@"
drop table [{BulkLoadTableName}];
";
            Log.Verbose("Dropping temporary table {tempTableName} with {CommandText}", BulkLoadTableName, dropTempTableCmd.CommandText);
            try
            {
                dropTempTableCmd.ExecuteNonQuery();
            } 
            catch (SqlException ex)
            {
                Log.Warning("Couldn't drop table {BulkLoadTableName}: {ex}", BulkLoadTableName, ex.Message);
            }
            base.Cleanup();
        }

        /// <summary>
        /// This method is called whenever a row is being removed from the input.
        /// </summary>
        /// <param name="row"></param>
        protected virtual void OnRowRemoved(string key, Row row)
        {

        }

        /// <summary>
        /// Inserts all rows having a valid <see cref="PrimaryKeyField"/> as Json Objects into a temporary table.
        /// </summary>
        private void InsertRecords()
        {
            using var createTempTableCmd = DbConnection.CreateCommand();
            createTempTableCmd.CommandText = $@"
create table [{BulkLoadTableName}] (
    Id nvarchar(200) Primary Key clustered,
    JsonObject nvarchar(max) not null
)
";
            Log.Verbose("Creating temporary table {tempTableName} with {CommandText}", BulkLoadTableName, createTempTableCmd.CommandText);
            createTempTableCmd.ExecuteNonQuery();
            // insert data into temp table
            using var dt = new DataTable();

            dt.Columns.Add("Id", typeof(string)); 
            dt.Columns.Add("JsonObject", typeof(string));
            while (Input.HasRowsAvailable)
            {
                var row = Input.RemoveRow();
                if (row.ContainsKey(PrimaryKeyField))
                {
                    var key = row[PrimaryKeyField]?.ToString();
                    if (!string.IsNullOrEmpty(key))
                    {
                        OnRowRemoved(key, row);
                        dt.Rows.Add(key, row.ToJson());
                    }
                }
            }
            using var bulkCopy = new SqlBulkCopy(DbConnection)
            {
                DestinationTableName = BulkLoadTableName,
                BatchSize = BatchSize,
                BulkCopyTimeout = Timeout
            };
            Log.Debug("Loading {records} records into {DestinationTableName}", dt.Rows.Count, bulkCopy.DestinationTableName);
            bulkCopy.WriteToServer(dt);
        }

        /// <summary>
        /// Upserts all records from the temporary table into table <see cref="TableName"/>. Merges records using the <see cref="PrimaryKeyField"/>.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "<Pending>")]
        protected virtual void MergeRecords()
        {
            using var cmd = DbConnection.CreateCommand();
            cmd.CommandTimeout = CommandTimeout;
            cmd.CommandText = $@"
merge [{TableName}] as target using [{BulkLoadTableName}]  as source on target.Id = source.Id
when matched then 
	update set JsonObject = source.JsonObject, UpdatedOn = CURRENT_TIMESTAMP
when not matched then
	insert (Id, JsonObject, UpdatedOn) values (source.Id, source.JsonObject, CURRENT_TIMESTAMP);
";
            Log.Debug("Merge data into {TableName}", TableName);
            Log.Verbose("Using {CommandText}", cmd.CommandText);
            int records = cmd.ExecuteNonQuery();
            Log.Debug("Merged {records} records.", records);
        }
    }
}
