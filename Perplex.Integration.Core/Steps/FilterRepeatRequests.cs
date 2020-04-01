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
    public class FilterRepeatRequests : SqlJsonSink, IDataSource
    {
        public IPipelineOutput Output { get; set; }

        private readonly IDictionary<string, Row> rows = new Dictionary<string, Row>();

        public FilterRepeatRequests()
        {
            //BulkLoadTableName = "TempJsonObjects";
        }

        public override void Initialise()
        {
            base.Initialise();
            rows.Clear();
        }
        protected override void OnRowRemoved(string key, Row row)
        {
            rows.Add(key, row);
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
select o1.Id from [{BulkLoadTableName}] o1 left join [{TableName}] o2 on o1.Id = o2.Id
where o2.Id is null or o1.JsonObject != o2.JsonObject;
";
            Log.Verbose("Retrieving changed objects using {CommandText}", cmd.CommandText);
            using var reader = cmd.ExecuteReader();
            long counter = 0;
            while(reader.Read())
            {
                var id = reader.GetString(0);
                Output.AddRow(rows[id]);
                counter++;
            }
            reader.Close();
            Log.Information("Retrieved {counter} changed records.", counter);
        }

    }
}
