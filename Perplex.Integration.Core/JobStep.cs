using Perplex.Integration.Core.Configuration;
using System;
using System.Reflection;

namespace Perplex.Integration.Core
{
    public abstract class JobStep : IDisposable
    {

        [Property("id", Required = true, Inline = true)]
        public string Id { get; set; }
        public string Type { get; private set; }

        public JobStep()
        {
            StepAttribute stepAttribute = GetType().GetCustomAttribute<StepAttribute>();
            if (stepAttribute != null)
            {
                Type = stepAttribute.Name ?? GetType().Name;
            }
        }

        /// <summary>
        /// Initialise the step.
        /// </summary>
        /// <remarks>
        /// Typically, open any connections, create any tables, etc.
        /// </remarks>
        public virtual void Initialise() { }
        /// <summary>
        /// Execute the step.
        /// </summary>
        /// <remarks>
        /// Remove rows from input and add to output, if applicable.
        /// </remarks>
        public abstract void Execute();
        /// <summary>
        /// Clean up your mess. This method may be called multiple times.
        /// </summary>
        /// <remarks>
        /// Close and dispose any connections, etc...
        /// </remarks>
        public virtual void Cleanup() { }
        /// <summary>
        /// Validate the configuration of a step before the first step in the job is run. 
        /// </summary>
        /// <exception cref="InvalidConfigurationException">Invalid configuration.</exception>
        public virtual void Validate() {}

        /// <summary>
        /// Dispose this step. Calls <see cref="Cleanup"/>.
        /// </summary>
        // Dispose() calls Dispose(true)
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // The bulk of the clean-up code is implemented in Dispose(bool)
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.Cleanup();
            }
        }
    }
}
