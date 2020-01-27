using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Configuration
{
    public class ConnectionStringAttribute : PropertyAttribute
    {
        public ConnectionStringAttribute(string name ) : base (name) { }
        
        public string Type { get; set; }
    }
}
