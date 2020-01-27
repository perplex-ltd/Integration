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
        protected const string TempTableName = "#TempJsonObjects";

        public IPipelineInput Input { get; set; }

        [IntegerProperty(MinValue = 0, MaxValue = 100000)]
        public int BatchSize { get; set; }
        [IntegerProperty(MinValue = 0, MaxValue = 100000, Description = "The timeout in seconds.")]
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
		JsonObject nvarchar(max) not null,
        UpdatedOn DateTime not null
	);
end
";
            cmd.Parameters.AddWithValue("@TableName", TableName);
            Log.Debug("Initialising table {TableName} with {CommandText}", TableName, cmd.CommandText);
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
            MergeRecords();

        }

        /// <summary>
        /// Inserts all rows as Json Objects into a temporary table.
        /// </summary>
        private void InsertRecords()
        {
            using var createTempTableCmd = DbConnection.CreateCommand();
            createTempTableCmd.CommandText = $@"
create table [{TempTableName}] (
    JsonObject nvarchar(max) not null
)
";
            Log.Debug("Creating temporary table {tempTableName} with {CommandText}", TempTableName, createTempTableCmd.CommandText);
            createTempTableCmd.ExecuteNonQuery();
            // insert data into temp table
            using var dt = new DataTable();

            dt.Columns.Add("JsonObject", typeof(string));
            while (Input.HasRowsAvailable)
            {
                dt.Rows.Add(Input.RemoveRow().ToJson());
            }
            using var bulkCopy = new SqlBulkCopy(DbConnection)
            {
                DestinationTableName = TempTableName,
                BatchSize = BatchSize,
                BulkCopyTimeout = Timeout
            };
            Log.Debug("Writing data to {DestinationTableName}", bulkCopy.DestinationTableName);
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
declare @JsonKeyPath as nvarchar(50) = '$.' + @KeyField;
merge [{TableName}] as target
using #TempJsonObjects as source 
on json_value(target.JsonObject, @JsonKeyPath) = json_value(source.JsonObject, @JsonKeyPath)
when matched then 
	update set JsonObject = source.JsonObject, UpdatedOn = CURRENT_TIMESTAMP
when not matched then
	insert (JsonObject, UpdatedOn)
	values (source.JsonObject, CURRENT_TIMESTAMP);
";
            cmd.Parameters.AddWithValue("@KeyField", PrimaryKeyField);
            Log.Debug("Merge data into {TableName} using {CommandText}", TableName, cmd.CommandText);
            cmd.ExecuteNonQuery();
        }
    }
}
