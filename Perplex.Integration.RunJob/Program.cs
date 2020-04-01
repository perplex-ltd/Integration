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
        IntegrationConfig config;
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
                config = ConfigFactory.Default.LoadFromFile(options.IntegrationConfigFile);

                if (options.ListJobs)
                {
                    ListJobs();
                }
                else
                {
                    RunJob();
                }
            }
            catch (InvalidConfigurationException ex)
            {
                Console.WriteLine($"Failed to load configuration: {ex.Message}");
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Exception while running job {Job}", options.Job);
            }
        }

        private void ListJobs()
        {
            const int padding = 30;
            Console.WriteLine($"Jobs defined in {options.IntegrationConfigFile}:");
            Console.WriteLine();
            Console.WriteLine("ID".PadRight(padding) + "Description");
            Console.WriteLine("--".PadRight(padding) + "-----------");
            foreach (var job in config.Jobs.Values)
            {
                Console.WriteLine($"{job.Id.PadRight(padding)}{job.Description}");
            }
            Console.WriteLine();
        }

        private void RunJob()
        {
            if (!config.Jobs.ContainsKey(options.Job))
            {
                Console.WriteLine($"Job {options.Job} is not defined in configuration");
                ListJobs();
                return;
            }
            var job = config.Jobs[options.Job];
            job.Run();
        }
    }
}
