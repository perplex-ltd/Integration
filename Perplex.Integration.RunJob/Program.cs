using CommandLine;
using Perplex.Integration.Core.Configuration;
using Serilog;
using Serilog.Exceptions;
using System;


namespace Perplex.Integration.RunJob
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Jigsaw Integration (c) Perplex Ltd 2020");
            try
            {
                Parser.Default.ParseArguments<Options>(args).WithParsed(o =>
                {
                    new Program(o).Run();
                });
            }
            catch (Exception ex)
            {
                // Display the details of the exception.
                Log.Fatal(ex, "Fatal exception");
            }
            if (System.Diagnostics.Debugger.IsAttached)
            {
                Console.WriteLine("Press any key to continue...");
                Console.ReadKey();
            }
        }

        readonly Options options;
        Program(Options options)
        {
            this.options = options;
        }

        private void Run()
        {
            Log.Logger = new LoggerConfiguration()
                .ReadFrom.AppSettings()
                .Enrich.WithExceptionDetails()
                .Enrich.FromLogContext()
                .MinimumLevel.Debug()
                .WriteTo.File("logs\\log-.txt", rollingInterval: RollingInterval.Day)
                .MinimumLevel.Is(options.LogLevel)
                .WriteTo.Console()
                .CreateLogger();
            try
            {
                IntegrationConfig config = ConfigFactory.Default.LoadFromFile(options.IntegrationConfigFile);
                if ( !config.Jobs.ContainsKey(options.Job))
                {
                    Log.Fatal("Job {Job} is not defined in configuration", options.Job);
                    return;
                }
                var job = config.Jobs[options.Job];
                job.Run();
            }
            catch (InvalidConfigurationException ex)
            {
                Log.Fatal(ex, "Failed to load configuration");
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Exception while running job {Job}", options.Job);
            }
        }

    }
}
