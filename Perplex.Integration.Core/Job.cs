using Perplex.Integration.Core.Internal;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core
{
    public class Job
    {

        private readonly IList<JobStep> steps = new List<JobStep>();

        public string Id { get; set; }

        public void AddStep(JobStep step)
        {
            // Add pipeline buffer object between data sink and data source
            if (step is IDataSink)
            {
                var lastStep = steps.LastOrDefault();
                if (!(lastStep is IDataSource))
                    throw new InvalidOperationException("Can only add a Data Sink after a Data Source.");
                var pipe = new PipelineBuffer();
                ((IDataSource)lastStep).Output = pipe;
                ((IDataSink)step).Input = pipe;
            }
            steps.Add(step);
        }


        /// <summary>
        /// Runs all steps in this job.
        /// </summary>
        public void Run()
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            Log.Information("Running Job {Id}", Id);
            ValidateJob();
            foreach (var step in steps)
            {
                try
                {
                    if (step is IDataSink sinkStep)
                    {
                        Log.Information("Running step {step} with {count} rows", step.Id, sinkStep.Input.Count);
                    }
                    else
                    {
                        Log.Information("Running step {step}", step.Id);
                    }
                    Log.Debug("Initialising...");
                    step.Initialise();
                    Log.Debug("Executing...");
                    step.Execute();
                    Log.Debug("Cleaning up...");
                    step.Cleanup();
                }
                catch (StepException ex)
                {
                    Log.Fatal("Error while running step {step.Id}: {ex.Message}", step.Id, ex.Message);
                }
                catch (Exception ex)
                {
                    Log.Fatal(ex, "Error while running step {step.Id}: {ex.Message}", step.Id, ex.Message);
                }
            }
            stopwatch.Stop();
            Log.Information("Job {jobId} finished in {stopwatch.Elapsed}.", Id, stopwatch.Elapsed);
        }

        private void ValidateJob()
        {
            if (steps.Count == 0)
                throw new InvalidOperationException("Job cannot be empty.");
            if (steps.Last() is IDataSource lastSource)
            {
                lastSource.Output = new NullPipelineOutput();
                //throw new InvalidOperationException("Last step in a job must not be a Data Source");
            }

            foreach (var step in steps)
            {
                try
                {
                    step.Validate();
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Validation failed for step {step}", step.Id);
                    throw;
                }
            }
        }
    }
}
