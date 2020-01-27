using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core
{
    /// <summary>
    /// Represents a component that has an input.
    /// </summary>
    public interface IDataSink
    {
        IPipelineInput Input { get; set; }
    }
}
