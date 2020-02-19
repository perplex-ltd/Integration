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
        Upsert,
        InsertOrUpdate
    }

    [Step(Description = "Dynamics 365 Sink")]
    public class CrmSink : AbstractCrmStep, IDataSink, IDataSource
    {
        [IntegerProperty(MinValue = 1, MaxValue = 1000)]
        public int MaxNumberOfRequests { get; set; } = 10;
        [Property()]
        public string EntityLogicalName { get; set; }
        [Property()]
        public bool TakeOperationFromRow { get; set; } = false;
        [Property()]
        public string OperationFieldName { get; set; } = "$operation";
        /// <summary>
        /// Specifies what operation should be carried out.
        /// Use Insert, Update, Upsert or Delete for the respective CRM requests.
        /// Use InsertOrUpdate to automatically insert or update data, depending on whether a row has a value for the primary key.
        /// </summary>
        [Property()]
        public Operation? Operation { get; set; } = Steps.Operation.InsertOrUpdate;
        [Property()]
        public string AlternateKey { get; set; }



        [Property()]
        public bool ContinueOnError { get; set; } = true;
        public IPipelineInput Input { get; set; }
        public IPipelineOutput Output { get; set; }

        private EntityMetadata entityMetadata;
        private int recordCounter;
        private string primaryKeyField = null;

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


        /// <summary>
        /// Uploaded all records to CRM.
        /// </summary>
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

        /// <summary>
        /// Converts all rows in <paramref name="batch"/> to CRM request and uploads them.
        /// </summary>
        /// <param name="batch"></param>
        private void UploadBatch(IList<Row> batch)
        {
            // Create request
            var requests = new List<OrganizationRequest>(MaxNumberOfRequests);
            foreach (var row in batch)
            {
                try
                {
                    requests.Add(CreateRequest(row));
                } 
                catch (Exception ex)
                {
                    Log.Error("{ex} while processing row {row}", ex.Message, row.ToJson());
                    if (!ContinueOnError) throw;
                }
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
                    recordCounter++;
                    switch (response.Response)
                    {
                        case CreateResponse cr: 
                            batch[i][primaryKeyField] = cr.id;
                            Output.AddRow(batch[i]);
                            break;
                        case UpsertResponse usr:
                            batch[i][primaryKeyField] = usr.Target.Id;
                            Output.AddRow(batch[i]);
                            break;
                        case UpdateResponse _:
                            Output.AddRow(batch[i]);
                            break;
                        case DeleteResponse _: 
                            break;
                    }
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
            if (row.ContainsKey(primaryKeyField) && row[primaryKeyField] != null && !(row[primaryKeyField] is DBNull))
            {
                try
                {
                    entity.Id = (Guid)row[primaryKeyField];
                } catch (InvalidCastException)
                {
                    throw new StepException(Id, $"'{primaryKeyField}' is not a valid guid.");
                }
            }
            if (operation == Steps.Operation.InsertOrUpdate)
            {
                operation = (entity.Id == Guid.Empty) ? Steps.Operation.Insert : Steps.Operation.Update;
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
            foreach (string column in row.Columns.Where(c => (c != AlternateKey) && (c != primaryKeyField)))
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

            if ((value == null)||(value is DBNull)) return null;
            var attribute = entityMetadata.Attributes.FirstOrDefault(a => a.LogicalName == attributeName);
            if (attribute == null)
                throw new StepException("Specified field {0} does not exist in entity {1}", attributeName, EntityLogicalName);
            try
            {
                switch (attribute.AttributeType)
                {
                    // Categorization data attributes
                    case AttributeTypeCode.Picklist:
                    case AttributeTypeCode.Status:
                        return ConvertValueToEnumAttributeValue(value, (EnumAttributeMetadata)attribute);
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
                    case AttributeTypeCode.Virtual:
                        if (attribute is MultiSelectPicklistAttributeMetadata mspAttribute) return ConvertToMultiSelectPickListValue(value, mspAttribute);
                        else throw new StepAttributeTypeNotSupportedException(Id, attribute);
                    case AttributeTypeCode.ManagedProperty: throw new StepAttributeTypeNotSupportedException(Id, attribute);
                    default: throw new StepException("Attribute {0} has an unknown data type.", attributeName);
                }
            }
            catch (InvalidCastException)
            {
                throw new StepException(Id, "Could not convert attribute {0} ({1}) into {2}.", attributeName, value.GetType().Name, attribute.AttributeTypeName);
            }
        }

        private OptionSetValue ConvertValueToEnumAttributeValue(object value, EnumAttributeMetadata attribute)
        {
            return (value is string label) ? ConvertStringToOptionSetValue(label, attribute) :
                                        new OptionSetValue(((IConvertible)value).ToInt32(System.Globalization.CultureInfo.InvariantCulture));
        }

        private OptionSetValueCollection ConvertToMultiSelectPickListValue(object value, MultiSelectPicklistAttributeMetadata attribute)
        {
            var osValue = ConvertValueToEnumAttributeValue(value, attribute);
            if (osValue == null) return null;
            var collection = new OptionSetValueCollection
            {
                osValue
            };
            return collection;
        }

        private OptionSetValue ConvertStringToOptionSetValue(string label, EnumAttributeMetadata attribute)
        {
            if (string.IsNullOrEmpty(label)) return null;
            var option = attribute.OptionSet.Options.FirstOrDefault(o => o.Label.UserLocalizedLabel.Label == label);
            if (option == null) throw new StepException(Id, $"No option set value with label '{label}' exists.");
            return new OptionSetValue(option.Value.Value);
        }
    }
}
