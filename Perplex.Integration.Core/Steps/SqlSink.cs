using System;

namespace Perplex.Integration.Core.Steps
{
    [Configuration.Step()]
    public class SqlSink : AbstractSqlStep
    {
        [Configuration.Property(Required = true)]
        public string SqlStatement { get; set; }


        public override void Execute()
        {
            throw new NotImplementedException();
        }

    }
}
