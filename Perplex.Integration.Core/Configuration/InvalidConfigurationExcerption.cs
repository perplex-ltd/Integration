using System;

namespace Perplex.Integration.Core.Configuration
{
    [Serializable()]
    public class InvalidConfigurationException : Exception
    {
        private InvalidConfigurationException() { }
        public InvalidConfigurationException(string message) : base(message) { }
        public InvalidConfigurationException(string message, Exception innerException) : base(message, innerException) { }
        public InvalidConfigurationException(string message, params object[] args) : base(string.Format(System.Globalization.CultureInfo.InvariantCulture, message, args)) { }

        protected InvalidConfigurationException(System.Runtime.Serialization.SerializationInfo serializationInfo, System.Runtime.Serialization.StreamingContext streamingContext) : 
            base(serializationInfo, streamingContext) { }
    }
}
