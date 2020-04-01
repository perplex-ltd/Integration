using Perplex.Integration.Core.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    public class FieldRemoveMapping
    {
        public FieldRemoveMapping() { }
        public FieldRemoveMapping(string field)
        {
            Field = field;
        }

        [Property(Inline = true, Required = true)]
        public string Field { get; set; }
    }
}
