using CommandLine;
using Serilog.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.RunJob
{
    class Options
    {

        [Option('c', "configuration", Default = "integrationconfig.xml", 
            HelpText = "The integration config file.",
           Required = false)]
        public string IntegrationConfigFile { get; set; }

        [Option('j', "job", Default = null, HelpText = "The job to run.",  SetName = "RunJob",
           Required = true)]
        public string Job { get; set; }

        [Option('l', "logLevel", Default = LogEventLevel.Debug, HelpText = "The minimum log level (Verbose, Debug, Information, Warning, Error or Fatal)")]
        public LogEventLevel LogLevel { get; internal set; }

        [Option('s', "show", Default = false, HelpText = "Show all defined jobs", Required = true, SetName = "ShowJobs")]
        public bool ListJobs { get; set;  }

    }
}
