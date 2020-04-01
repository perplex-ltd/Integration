using Perplex.Integration.Core.Configuration;
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
        public object Description { get; set; }
        /// <summary>
        /// Adds a step to the job and connects it to the last step, if necessary.
        /// </summary>
        /// <param name="step"></param>
        public void AddStep(JobStep step)
        {
            if (step is IDataSource source)
            {
                source.Output = new OutputPipeline();
            }
            var lastStep = steps.LastOrDefault();
            ConnectAndAddStep(step, lastStep);
        }

        /// <summary>
        /// Add a step to the job and connects it to <paramref name="sourceStepId"/>.
        /// </summary>
        /// <param name="step">The step to add.</param>
        /// <param name="sourceStepId">The Id of the data source step.</param>
        /// <exception cref="InvalidConfigurationException">sourceStepId doesn't exist.</exception>
        public void AddStep(JobStep step, string sourceStepId)
        {
            if (step is null)
                throw new ArgumentNullException(nameof(step));
            var sourceStep = steps.FirstOrDefault(s => s.Id == sourceStepId);
            if (sourceStep is null) 
                throw new InvalidConfigurationException($"Data Source step {sourceStepId} doesn't exist.");
            if (!(sourceStep is IDataSource))
                throw new InvalidConfigurationException($"Data Source Step {sourceStepId} is not a data source.");
            if (!(step is IDataSink))
                throw new InvalidConfigurationException($"Step {step.Id} is not a Data Sink.");
            ConnectAndAddStep(step, sourceStep);
        }

        private void ConnectAndAddStep(JobStep step, JobStep sourceStep)
        {
            if (step is IDataSink)
            {
                if (!(sourceStep is IDataSource lastSource))
                    throw new InvalidOperationException($"Step {sourceStep.Id} must be a Data Source to be connected to {step.Id}.");
                ((IDataSink)step).Input = ((OutputPipeline)lastSource.Output).CreateInputPipeline();
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
                        Log.Information("Running step {step} ({type})", step.Id, step.Type);
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
                    Log.Fatal("Error while running step {step}: {ex}", step.Id, ex.Message);
                    break;
                }
                catch (Exception ex)
                {
                    Log.Fatal(ex, "Unexpected error while running step {step}: {ex}", step.Id, ex.Message);
                    break; ;
                }
            }
            stopwatch.Stop();
            Log.Information("Job {jobId} finished in {elapsed}.", Id, stopwatch.Elapsed);
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
