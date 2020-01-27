using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Configuration
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class StepAttribute : System.Attribute
    {

        public StepAttribute() { }
        public StepAttribute(string name)
        {
            Name = name;
        }

        public string Name { get; private set; }
        public string Description { get; set; }
    }
}
