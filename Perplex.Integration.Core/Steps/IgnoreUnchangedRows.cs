using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Tooling.Connector;
using Newtonsoft.Json;
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

    [Configuration.Step( Description = "Filter out any unchanged rows.")]
    public class IgnoreUnchangedRows : SqlJsonSink, IDataSource
    {
        public IPipelineOutput Output { get; set; }


        public IgnoreUnchangedRows()
        {
        }

        /// <summary>
        /// Adds any records that are different from the ones in <see cref="TableName"/> to the Output.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "<Pending>")]
        protected override void MergeRecords()
        {
            using var cmd = DbConnection.CreateCommand();
            cmd.CommandTimeout = CommandTimeout;
            cmd.CommandText = $@"
declare @JsonKeyPath as nvarchar(50) = '$.' + @KeyField;
select o1.JsonObject from [{TempTableName}] o1
left join [{TableName}] o2 on json_value(o1.JsonObject, @JsonKeyPath) = json_value(o2.JsonObject, @JsonKeyPath)
where
    json_value(o2.JsonObject, @JsonKeyPath) is null or
	o1.JsonObject != o2.JsonObject;
";
            cmd.Parameters.AddWithValue("@KeyField", PrimaryKeyField);
            Log.Debug("Retrieving changed objects using {CommandText}", cmd.CommandText);
            using var reader = cmd.ExecuteReader();
            while(reader.Read())
            {
                var jsonObject = reader.GetString(0);
                Log.Verbose("Read {jsonObject}", jsonObject);
                var row = Row.FromJson(jsonObject);
                Output.AddRow(row);
            }
        }

    }
}
