using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core
{
    public abstract class DataProcessStep : JobStep, IDataSource, IDataSink
    {
        public IPipelineOutput Output { get; set; }
        public IPipelineInput Input { get; set; }
    }
}
