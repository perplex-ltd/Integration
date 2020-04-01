using Perplex.Integration.Core.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    public class FieldRenameMapping
    {
        public FieldRenameMapping() { }
        public FieldRenameMapping(string from, string to)
        {
            From = from;
            To = to;
        }

        [Property(Inline = true, Required = true)]
        public string From { get; set; }
        [Property(Inline = true, Required = true)]
        public string To { get; set; }
    }
}
