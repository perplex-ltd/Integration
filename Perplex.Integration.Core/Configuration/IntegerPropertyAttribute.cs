using System;

namespace Perplex.Integration.Core.Configuration
{

    public class IntegerPropertyAttribute : PropertyAttribute
    {
        public IntegerPropertyAttribute() : base() { }
        public IntegerPropertyAttribute(string name) : base(name) { }

        public int MinValue { get; set; }
        public int MaxValue { get; set; }

    }
}