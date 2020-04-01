using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    public abstract class AbstractCrmBatchRequestStep : AbstractCrmStep, IDataSink, IDataSource
    {

        [IntegerProperty(MinValue = 1, MaxValue = 1000)]
        public int MaxNumberOfRequests { get; set; } = 10;
        [Property()]
        public string EntityLogicalName { get; set; }
        [Property()]
        public bool ContinueOnError { get; set; } = true;
        public IPipelineInput Input { get; set; }
        public IPipelineOutput Output { get; set; }

        protected int SuccessCounter { get; private set; }
        protected int ErrorCounter { get; private set; }
        protected int TotalCounter { get; private set; }


        private EntityMetadata entityMetadata;


        protected string PrimaryKeyField { get; private set; } = null;

        public override void Initialise()
        {
            base.Initialise();
            SuccessCounter = 0;
            ErrorCounter = 0;
            TotalCounter = 0;
            PrimaryKeyField = EntityLogicalName + "id";
            entityMetadata = CrmServiceClient.GetEntityMetadata(EntityLogicalName, EntityFilters.Attributes | EntityFilters.Relationships);
        }

        /// <summary>
        /// Uploaded all records to CRM.
        /// </summary>
        public override void Execute()
        {
            // prepare rows needing to be uploaded
            var batch = new List<(Row, OrganizationRequest)>(MaxNumberOfRequests);
            while (Input.HasRowsAvailable)
            {
                var row = Input.RemoveRow();
                // convert to request
                OrganizationRequest request = null;
                try
                {
                    request = CreateRequest(row);
                }
                catch (StepException ex )
                {
                    ErrorCounter++;
                    Log.Error("Error '{ex}' while creating CRM request for row {row}", ex.Message, row.ToJson());
                    if (!ContinueOnError) throw;
                }
                catch (Exception ex)
                {
                    ErrorCounter++;
                    Log.Error("Unexpected error '{ex}' while creating CRM request for row {row}", ex.Message, row.ToJson());
                    if (!ContinueOnError) throw;
                }
                if (request != null)
                {
                    batch.Add((row, request));
                    if (batch.Count >= MaxNumberOfRequests)
                    {
                        ExecuteBatch(batch);
                        batch.Clear();
                    }
                }
                TotalCounter++;
                if (TotalCounter % 500 == 0)
                {
                    Log.Information("Processed {TotalCounter} records so far", TotalCounter);
                }
            }
            if (batch.Count > 0)
            {
                ExecuteBatch(batch);
            }
            Log.Information("Processesed {TotalCounter} records, executed {recordCounter} successful requests and encountered {errorCounter} errors",
                TotalCounter, SuccessCounter, ErrorCounter);
        }

        /// <summary>
        /// Converts all rows in <paramref name="batch"/> to CRM request and uploads them.
        /// </summary>
        /// <param name="batch"></param>
        private void ExecuteBatch(IList<(Row,OrganizationRequest)> batch)
        {
            // Create request
            var executeMultipleRequest = new ExecuteMultipleRequest()
            {
                Settings = new ExecuteMultipleSettings()
                {
                    ContinueOnError = ContinueOnError,
                    ReturnResponses = true
                },
                Requests = new OrganizationRequestCollection()
            };
            executeMultipleRequest.Requests.AddRange(batch.Select(item => item.Item2));
            // execute
            Log.Debug("Executing {Count} CRM requests", executeMultipleRequest.Requests.Count);
            var executeMultipleResponse =
                (ExecuteMultipleResponse)CrmServiceClient.Execute(executeMultipleRequest);
            // check response and collect updated objects
            foreach (var response in executeMultipleResponse.Responses)
            {
                Row processedRow = batch[response.RequestIndex].Item1;
                if (response.Fault != null)
                {
                    ErrorCounter++;
                    Log.Error("Request failed for row {row} with fault {Fault}", processedRow, response.Fault);
                }
                else
                {
                    SuccessCounter++;
                    ProcessReponse(response.Response, processedRow, 
                        executeMultipleRequest.Requests[response.RequestIndex]);
                }
            }
            if (ErrorCounter == 0)
            {
                Log.Information("Executed {recordCounter} successful requests", SuccessCounter);
            }
            else
            {
                Log.Information("Executed {recordCounter} successful requests and {errorCounter} errors", SuccessCounter, ErrorCounter);
            }
        }

        /// <summary>
        /// Create an Organization Request that is eventually passed into a ExecuteMultiple request.
        /// </summary>
        /// <param name="row">The current row</param>
        /// <returns>A valid Organization request or null.</returns>
        protected abstract OrganizationRequest CreateRequest(Row row);

        /// <summary>
        /// Process a successful response from the ExecuteMultiple Request.
        /// </summary>
        /// <param name="response">The organisation response for the request.</param>
        /// <param name="processedRow">The original row.</param>
        /// <param name="request">The original request.</param>
        protected abstract void ProcessReponse(OrganizationResponse response, Row processedRow, OrganizationRequest request);

        protected AttributeMetadata GetAttributeMetadata(string attributeName)
        {
            return entityMetadata.Attributes.FirstOrDefault(a => a.LogicalName == attributeName);
        }

        protected Relationship GetRelationshipByLookupAttribute(string lookupField, string entityName)
        {
            var relationshipMetadata = entityMetadata.ManyToOneRelationships.FirstOrDefault(r => r.ReferencingAttribute == lookupField && r.ReferencedEntity == entityName);
            if (relationshipMetadata == null) throw new StepException($"Cannot find many-to-one relationship metadata for {lookupField}.");
            return new Relationship(relationshipMetadata.SchemaName);
        }
    }
}
