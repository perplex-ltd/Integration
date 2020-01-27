using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Tooling.Connector;
using Newtonsoft.Json;
using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    [Serializable]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1032:Implement standard exception constructors", Justification = "<Pending>")]
    public class StepAttributeTypeNotSupportedException : StepException
    {
        public StepAttributeTypeNotSupportedException(string stepId, AttributeMetadata attribute)
            : base(stepId, string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0} attributes are not supported ({1})",
                attribute.AttributeType, attribute.LogicalName))
        { }

        protected StepAttributeTypeNotSupportedException(System.Runtime.Serialization.SerializationInfo serializationInfo, System.Runtime.Serialization.StreamingContext streamingContext)
        : base(serializationInfo, streamingContext) { }
    }

    public enum Operation
    {
        Insert,
        Update,
        Delete,
        Upsert
    }

    [Step(Description = "Dynamics 365 Sink")]
    public class CrmSink : AbstractCrmStep, IDataSink, IDataSource
    {
        [IntegerProperty(MinValue = 1, MaxValue = 1000)]
        public int MaxNumberOfRequests { get; set; }
        [Property()]
        public string EntityLogicalName { get; set; }
        [Property()]
        public bool TakeOperationFromRow { get; set; }
        [Property()]
        public string OperationFieldName { get; set; }
        [Property()]
        public Operation? Operation { get; set; }
        [Property()]
        public string AlternateKey { get; set; }



        [Property()]
        public bool ContinueOnError { get; set; }
        public IPipelineInput Input { get; set; }
        public IPipelineOutput Output { get; set; }

        private EntityMetadata entityMetadata;
        private int recordCounter;
        private string primaryKeyField = null;

        public CrmSink()
        {
            MaxNumberOfRequests = 10;
            ContinueOnError = true;
            TakeOperationFromRow = false;
            OperationFieldName = "$op";
            Timeout = 600;
        }

        public override void Validate()
        {
            base.Validate();
            if ((!TakeOperationFromRow) && (!Operation.HasValue)) throw new InvalidConfigurationException("Must specify Operation property if TakeOperationFromRow is null.");
            if ((Operation == Steps.Operation.Upsert) && (string.IsNullOrEmpty(AlternateKey))) throw new InvalidConfigurationException("Must specify an AlternateKey if operation is upsert.");
        }
        public override void Initialise()
        {
            base.Initialise();
            primaryKeyField = EntityLogicalName + "id";
            // CRM connection
            entityMetadata = CrmServiceClient.GetEntityMetadata(EntityLogicalName, EntityFilters.Attributes);
            recordCounter = 0;
        }

        public override void Execute()
        {
            // prepare rows needing to be uploaded
            var batch = new List<Row>(MaxNumberOfRequests);
            while (Input.HasRowsAvailable)
            {
                var row = Input.RemoveRow();
                batch.Add(row);
                if (batch.Count >= MaxNumberOfRequests)
                {
                    UploadBatch(batch);
                    batch = new List<Row>();
                }
            }
            if (batch.Count > 0)
            {
                UploadBatch(batch);
            }
        }

        private void UploadBatch(IList<Row> batch)
        {
            // Create request
            var requests = new List<OrganizationRequest>(MaxNumberOfRequests);
            foreach (var row in batch)
            {
                requests.Add(CreateRequest(row));
            }
            var executeMultipleRequest = new ExecuteMultipleRequest()
            {
                Settings = new ExecuteMultipleSettings()
                {
                    ContinueOnError = ContinueOnError,
                    ReturnResponses = true
                },
                Requests = new OrganizationRequestCollection()
            };
            executeMultipleRequest.Requests.AddRange(requests);
            // execute
            Log.Debug("Executing {Count} multiple CRM requests", executeMultipleRequest.Requests.Count);
            var executeMultipleResponse =
                (ExecuteMultipleResponse)CrmServiceClient.Execute(executeMultipleRequest);
            // check response and collect updated objects
            for (int i = 0; i < executeMultipleResponse.Responses.Count; i++)
            {
                var response = executeMultipleResponse.Responses[i];
                if (response.Fault != null)
                {
                    Log.Error("Request failed for row {row} with fault {Fault}", batch[i], response.Fault);
                }
                else
                {
                    Output.AddRow(batch[i]);
                    recordCounter++;
                }
            }
            Log.Information("Uploaded {recordCounter} records", recordCounter);
        }

        private OrganizationRequest CreateRequest(Row row)
        {
            var operation = (TakeOperationFromRow) ?
                (Operation)Enum.Parse(typeof(Operation), (string)row[OperationFieldName], true) :
                Operation.Value;
            // create entity object with alternate key, if it is specified.
            Entity entity =
                string.IsNullOrEmpty(AlternateKey) ? new Entity(EntityLogicalName) :
                new Entity(EntityLogicalName, AlternateKey, GetCrmValue(AlternateKey, row[AlternateKey]));
            // add primary key field (e.g. contactid) if it exists in row
            if (row.ContainsKey(primaryKeyField) && row[primaryKeyField] != null)
            {
                entity.Id = (Guid)row[primaryKeyField];
            }
            // if this is a delete operation, we don't need to add attributes
            if (operation == Steps.Operation.Delete)
            {
                return new DeleteRequest()
                {
                    Target = entity.ToEntityReference()
                };
            }
            // add all attributes apart from primary key and alternate key
            foreach (string column in row.Columns.Where(c => (c != AlternateKey) || (c != primaryKeyField)))
            {
                entity[column] = GetCrmValue(column, row[column]);
            }
            return operation switch
            {
                Steps.Operation.Insert => new CreateRequest() { Target = entity },
                Steps.Operation.Update => new UpdateRequest() { Target = entity },
                Steps.Operation.Upsert => new UpsertRequest() { Target = entity },
                _ => throw new StepException(Id, "Invalid operation specified"),
            };
        }

        /// <summary>
        /// Converts a value into a value CRM will accept. 
        /// </summary>
        /// <param name="attributeName"></param>
        /// <param name="value"></param>
        /// <seealso cref="https://docs.microsoft.com/en-us/dynamics365/customerengagement/on-premises/developer/introduction-to-entity-attributes"/>
        /// <returns></returns>
        private object GetCrmValue(string attributeName, object value)
        {

            if (value == null) return null;
            var attribute = entityMetadata.Attributes.FirstOrDefault(a => a.LogicalName == attributeName);
            if (attribute == null)
                throw new StepException("Specified field {0} does not exist in entity {1}", attributeName, EntityLogicalName);
            try
            {
                switch (attribute.AttributeType)
                {
                    // Categorization data attributes
                    case AttributeTypeCode.Picklist:
                    case AttributeTypeCode.Status: return new OptionSetValue(((IConvertible)value).ToInt32(System.Globalization.CultureInfo.InvariantCulture));
                    case AttributeTypeCode.State: throw new StepAttributeTypeNotSupportedException(Id, attribute);
                    case AttributeTypeCode.Boolean: return (bool)value;
                    case AttributeTypeCode.EntityName: throw new StepAttributeTypeNotSupportedException(Id, attribute);
                    // Collection data attributes
                    case AttributeTypeCode.CalendarRules: throw new StepAttributeTypeNotSupportedException(Id, attribute);
                    case AttributeTypeCode.PartyList: throw new StepAttributeTypeNotSupportedException(Id, attribute);
                    // Date and time attributes
                    case AttributeTypeCode.DateTime: return (DateTime)value;
                    // Image data attributes identify as virtual attributes
                    // Quantity data attributes
                    case AttributeTypeCode.BigInt: throw new StepAttributeTypeNotSupportedException(Id, attribute);// BigInt attributes are for internal use only
                    case AttributeTypeCode.Decimal: return (decimal)value;
                    case AttributeTypeCode.Double: return (double)value;
                    case AttributeTypeCode.Integer: return ((IConvertible)value).ToInt32(System.Globalization.CultureInfo.InvariantCulture);
                    case AttributeTypeCode.Money: return new Money((decimal)value);
                    // Reference data attributes
                    // TODO: Reference attributes
                    case AttributeTypeCode.Customer:
                    case AttributeTypeCode.Lookup:
                    case AttributeTypeCode.Owner: throw new StepAttributeTypeNotSupportedException(Id, attribute);
                    // String data attributes
                    case AttributeTypeCode.String:
                    case AttributeTypeCode.Memo: return (string)value;
                    // Unique identifier data attributes
                    case AttributeTypeCode.Uniqueidentifier: throw new StepAttributeTypeNotSupportedException(Id, attribute);
                    // Virtual attributes
                    case AttributeTypeCode.Virtual: throw new StepAttributeTypeNotSupportedException(Id, attribute);
                    case AttributeTypeCode.ManagedProperty: throw new StepAttributeTypeNotSupportedException(Id, attribute);
                    default: throw new StepException("Attribute {0} has an unknown data type.", attributeName);
                }
            }
            catch (InvalidCastException)
            {
                throw new StepException(Id, "Could not convert attribute {0} ({1}) into {2}.", attributeName, value.GetType().Name, attribute.AttributeTypeName);
            }
        }

    }
}
