using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core
{
    public abstract class DataSinkStep : JobStep, IDataSink
    {
        public IPipelineInput Input { get; set; }
    }
}
