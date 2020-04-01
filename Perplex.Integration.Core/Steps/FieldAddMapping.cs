using Perplex.Integration.Core.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    public class FieldAddMapping
    {
        public FieldAddMapping() { }
        public FieldAddMapping(string field, string value)
        {
            Field = field;
            Value = value;
        }

        [Property(Inline = true, Required = true)]
        public string Field { get; set; }
        [Property(Inline = true, Required = true)]
        public string Value { get; set; }
    }
}
