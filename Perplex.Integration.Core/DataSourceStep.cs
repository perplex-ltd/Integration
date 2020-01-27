using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core
{
    public abstract class DataSourceStep : JobStep, IDataSource
    {
        public IPipelineOutput Output { get; set; }
    }
}
