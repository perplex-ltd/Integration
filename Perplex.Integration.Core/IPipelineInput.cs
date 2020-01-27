using System;
using System.Collections.Generic;

namespace Perplex.Integration.Core
{

    public class NoMoreRowsException : Exception
    {
        public NoMoreRowsException() { }
    }

    public interface IPipelineInput
    {
        bool HasRowsAvailable { get; }

        Row RemoveRow();
        public long Count { get; }
    }

    
}