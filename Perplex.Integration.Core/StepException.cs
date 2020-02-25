using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core
{
    /// <summary>
    /// Represents an exception that occured while a step is being run and prevents the step from being completed.
    /// </summary>
    [Serializable]
    public class StepException : Exception
    {
        public StepException()
        {
        }
        public StepException(string message) : base(message)
        {
        }
        public StepException(string message, Exception innerException) : base(message, innerException)
        {
        }
        protected StepException(System.Runtime.Serialization.SerializationInfo serializationInfo, System.Runtime.Serialization.StreamingContext streamingContext) : base(serializationInfo, streamingContext) { }

    }
}
