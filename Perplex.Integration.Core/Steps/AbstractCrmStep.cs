using Microsoft.Xrm.Tooling.Connector;
using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    public abstract class AbstractCrmStep : JobStep
    {
        [ConnectionString("CRM", Type = "Dynamics365")]
        public string CrmConnectionString { get; set; }
        [Property(Description = "Timeout in seconds")]
        public int Timeout { get; set; }

        protected CrmServiceClient CrmServiceClient { get; private set; }

        public AbstractCrmStep()
        {
            Timeout = 600;
        }

        public override void Initialise()
        {
            base.Initialise();
            // CRM connection
            Log.Debug("Connecting to CRM.");
            CrmServiceClient = new CrmServiceClient(CrmConnectionString);
            CrmServiceClient.OrganizationServiceProxy.Timeout = new TimeSpan(0, 0, Timeout);
        }

        public override void Cleanup()
        {
            CrmServiceClient?.Dispose();
            _ = CrmServiceClient == null;
        }

    }
}
