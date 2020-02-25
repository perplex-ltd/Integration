using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Perplex.Integration.Core
{

    [Serializable]
    public class NoMoreRowsException : Exception
    {
        public NoMoreRowsException() { }

        public NoMoreRowsException(string message) : base(message)
        {
        }

        public NoMoreRowsException(string message, Exception innerException) : base(message, innerException)
        {
        }
        protected NoMoreRowsException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    public interface IPipelineInput
    {
        bool HasRowsAvailable { get; }

        Row RemoveRow();
        public long Count { get; }
    }

    
}