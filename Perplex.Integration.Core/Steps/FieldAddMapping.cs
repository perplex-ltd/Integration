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
        /// <summary>
        /// The raw value. Use GetValue instead!
        /// </summary>
        [Property(Inline = true, Required = true)]
        public string Value { get; set; }
        
        /// <summary>
        ///  Returns the parsed value. 
        ///  Currently, only the formula =now() is supported, which replaces the value with the current date.
        /// </summary>
        /// <returns></returns>
        public object GetValue()
        {
            if ("=now()".Equals(Value, StringComparison.InvariantCultureIgnoreCase))
            {
                return DateTime.Now;
            } 
            else
            {
                return Value;
            }
        }

    }
}
