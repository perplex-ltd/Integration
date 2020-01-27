using Perplex.Integration.Core.Configuration;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    public abstract class AbstractSqlStep : JobStep
    {
        [ConnectionString("LocalDB", Type = "Sql")]
        public string DbConnectionString { get; set; }
        [Property()]
        public int CommandTimeout { get; set; }
        protected SqlConnection DbConnection { get; private set; }

        public AbstractSqlStep()
        {
            CommandTimeout = 300;
        }

        /// <summary>
        /// Creates and opens an Sql Connection.
        /// </summary>
        public override void Initialise()
        {
            base.Initialise();
            // SQL DB init stuff
            DbConnection = new SqlConnection(DbConnectionString);
            DbConnection.Open();
        }


        /// <summary>
        /// Closes the SQL connection, if it is open.
        /// </summary>
        public override void Cleanup()
        {
            base.Cleanup();
            DbConnection?.Close();
            DbConnection = null;
        }
    }
}
