using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core
{
    [Serializable]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1032:Implement standard exception constructors", Justification = "<Pending>")]
    public class StepException : Exception
    {
        public string StepId { get; private set; }
        public StepException(string stepId, string message) : base(message)
        {
            StepId = stepId;
        }
        public StepException(string stepId, string message, Exception innerException) : base(message, innerException)
        {
            StepId = stepId;
        }
        public StepException(string stepId, string message, params object[] args) : base(string.Format(CultureInfo.InvariantCulture, message, args))
        {
            StepId = stepId;
        }

        protected StepException(System.Runtime.Serialization.SerializationInfo serializationInfo, System.Runtime.Serialization.StreamingContext streamingContext) : base(serializationInfo, streamingContext) { }
    }
}
