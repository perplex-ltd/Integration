using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core
{
    /// <summary>
    /// Represents a component with an Output.
    /// </summary>
    public interface IDataSource
    {

        IPipelineOutput Output { get; set; }
    }
}
