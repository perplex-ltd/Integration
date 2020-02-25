using System;

namespace Perplex.Integration.Core.Configuration
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class PropertyAttribute : Attribute
    {
        public PropertyAttribute() : this(null) { }
        public PropertyAttribute(string name)  {
            Name = name;
            Required = false;
            Inline = false;
        }

        public string Name { get; set; }
        
        public bool Required { get; set; }

        public bool Inline { get; set; }

        public string Description { get; set; }
        public object Default { get; set; }
    }
}