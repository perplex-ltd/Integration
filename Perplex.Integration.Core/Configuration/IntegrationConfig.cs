using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Configuration
{
    public class IntegrationConfig 
    {
        public IntegrationConfig()
        {
            Jobs = new Dictionary<string, Job>();
        }

        public IDictionary<string, Job> Jobs { get; }

    }
}
