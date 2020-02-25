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
    public class StepAttributeTypeNotSupportedException : StepException
    {

        protected StepAttributeTypeNotSupportedException(System.Runtime.Serialization.SerializationInfo serializationInfo, System.Runtime.Serialization.StreamingContext streamingContext)
        : base(serializationInfo, streamingContext) { }

        public StepAttributeTypeNotSupportedException()
        {
        }

        public StepAttributeTypeNotSupportedException(string message) : base(message)
        {
        }

        public StepAttributeTypeNotSupportedException(string message, Exception innerException) : base(message, innerException)
        {
        }
        internal StepAttributeTypeNotSupportedException(AttributeMetadata attribute)
    : base(string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0} attributes are not supported ({1})",
        attribute.AttributeType, attribute.LogicalName))
        { }

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
        [Property]
        public bool TrimLongStrings { get; set; } = false;
        [Property(Description = "If set to true, fields that cannot be converted won't be uploaded, otherwise, the record will be skipped.")]
        public bool IgnoreFieldConversionErrors { get; set; } = false;



        [Property()]
        public bool ContinueOnError { get; set; } = true;
        public IPipelineInput Input { get; set; }
        public IPipelineOutput Output { get; set; }

        private EntityMetadata entityMetadata;
        private int recordCounter;
        private int errorCounter;
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
            errorCounter = 0;
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
                    errorCounter++;
                    Log.Error("Error '{ex}' while creating CRM request for row {row}", ex.Message, row.ToJson());
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
                    errorCounter++;
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
            if (errorCounter == 0)
            {
                Log.Information("Uploaded {recordCounter} records", recordCounter);
            }
            else
            {
                Log.Information("Uploaded {recordCounter} records and encountered {errorCounter} errors", recordCounter, errorCounter);
            }
        }

        private OrganizationRequest CreateRequest(Row row)
        {
            var operation = (TakeOperationFromRow) ?
                (Operation)Enum.Parse(typeof(Operation), (string)row[OperationFieldName], true) :
                Operation.Value;
            // create entity object with alternate key, if it is specified.
            Entity entity =
                string.IsNullOrEmpty(AlternateKey) ? new Entity(EntityLogicalName) :
                new Entity(EntityLogicalName, AlternateKey, GetCrmValue(AlternateKey, row[AlternateKey], null));
            // add primary key field (e.g. contactid) if it exists in row
            if (row.ContainsKey(primaryKeyField) && row[primaryKeyField] != null && !(row[primaryKeyField] is DBNull))
            {
                try
                {
                    entity.Id = (Guid)row[primaryKeyField];
                }
                catch (InvalidCastException)
                {
                    throw new StepException($"'{primaryKeyField}' is not a valid guid.");
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
            foreach (string column in row.Columns.Where(
                c => (c != AlternateKey) && (c != primaryKeyField) && !c.Contains("$")))
            {
                // get meta attributes (i.e. the ones with $ in their name, used for Lookup attributes)
                var metaValues = new Dictionary<string, object>();
                foreach (var mv in row.Columns.Where(mc => mc.StartsWith($"{column}$", StringComparison.InvariantCultureIgnoreCase)).Select(mc => new { Column = mc.Replace($"{column}$", string.Empty), Value = row[mc] }))
                {
                    metaValues.Add(mv.Column, mv.Value);
                }
                try
                {
                    entity[column] = GetCrmValue(column, row[column], metaValues);
                }
                catch (Exception ex)
                {
                    if (IgnoreFieldConversionErrors)
                    {
                        Log.Warning("Couldn't convert field {column} ('{value}') to CRM attribute: {exMessage}", column, row[column], ex.Message);
                    }
                    else
                    {
                        throw new StepException($"Couldn't convert field {column} ('{row[column]}') to CRM attribute: {ex.Message}");
                    }
                }
            }
            return operation switch
            {
                Steps.Operation.Insert => new CreateRequest() { Target = entity },
                Steps.Operation.Update => new UpdateRequest() { Target = entity },
                Steps.Operation.Upsert => new UpsertRequest() { Target = entity },
                _ => throw new StepException("Invalid operation specified"),
            };
        }

        /// <summary>
        /// Converts a value into a value CRM will accept. 
        /// </summary>
        /// <param name="attributeName"></param>
        /// <param name="value"></param>
        /// <seealso cref="https://docs.microsoft.com/en-us/dynamics365/customerengagement/on-premises/developer/introduction-to-entity-attributes"/>
        /// <returns></returns>
        private object GetCrmValue(string attributeName, object value, Dictionary<string, object> metaValues)
        {

            if ((value == null) || (value is DBNull)) return null;
            var attribute = entityMetadata.Attributes.FirstOrDefault(a => a.LogicalName == attributeName);
            if (attribute == null)
                throw new StepException($"Field {attributeName} does not exist in entity {EntityLogicalName}");
            try
            {
                switch (attribute.AttributeType)
                {
                    // Categorization data attributes
                    case AttributeTypeCode.Picklist:
                    case AttributeTypeCode.Status:
                        return ConvertValueToEnumAttributeValue(value, (EnumAttributeMetadata)attribute);
                    case AttributeTypeCode.State: throw new StepAttributeTypeNotSupportedException(attribute);
                    case AttributeTypeCode.Boolean:
                        return ConvertValueToBooleanAttributeValue(value, (BooleanAttributeMetadata)attribute);
                    case AttributeTypeCode.EntityName: throw new StepAttributeTypeNotSupportedException(attribute);
                    // Collection data attributes
                    case AttributeTypeCode.CalendarRules: throw new StepAttributeTypeNotSupportedException(attribute);
                    case AttributeTypeCode.PartyList: throw new StepAttributeTypeNotSupportedException(attribute);
                    // Date and time attributes
                    case AttributeTypeCode.DateTime: return (DateTime)value;
                    // Image data attributes identify as virtual attributes
                    // Quantity data attributes
                    case AttributeTypeCode.BigInt: throw new StepAttributeTypeNotSupportedException(attribute);// BigInt attributes are for internal use only
                    case AttributeTypeCode.Decimal: return (decimal)value;
                    case AttributeTypeCode.Double: return (double)value;
                    case AttributeTypeCode.Integer: return ((IConvertible)value).ToInt32(System.Globalization.CultureInfo.InvariantCulture);
                    case AttributeTypeCode.Money: return new Money(((IConvertible)value).ToDecimal(System.Globalization.CultureInfo.InvariantCulture));
                    // Reference data attributes
                    // TODO: Reference attributes
                    case AttributeTypeCode.Customer:
                        if (!metaValues.ContainsKey("type"))
                            throw new StepException($"Must specify target type in {attribute.LogicalName}$type column for Customer attributes");
                        var target = (string)metaValues["type"];
                        return (metaValues.ContainsKey("key")) ? new EntityReference(target, (string)metaValues["key"], value) :
                            new EntityReference(target, (Guid)value);
                    case AttributeTypeCode.Lookup:
                        target = ((LookupAttributeMetadata)attribute).Targets.FirstOrDefault();
                        return (metaValues.ContainsKey("key")) ? new EntityReference(target, (string)metaValues["key"], value) :
                            new EntityReference(target, (Guid)value);
                    case AttributeTypeCode.Owner: throw new StepAttributeTypeNotSupportedException(attribute);
                    // String data attributes
                    case AttributeTypeCode.String:
                        return ConvertString((string)value, ((StringAttributeMetadata)attribute).MaxLength.Value);
                    case AttributeTypeCode.Memo:
                        return ConvertString((string)value, ((MemoAttributeMetadata)attribute).MaxLength.Value);
                    // Unique identifier data attributes
                    case AttributeTypeCode.Uniqueidentifier: throw new StepAttributeTypeNotSupportedException(attribute);
                    // Virtual attributes
                    case AttributeTypeCode.Virtual:
                        if (attribute is MultiSelectPicklistAttributeMetadata mspAttribute) return ConvertToMultiSelectPickListValue(value, mspAttribute);
                        else throw new StepAttributeTypeNotSupportedException(attribute);
                    case AttributeTypeCode.ManagedProperty: throw new StepAttributeTypeNotSupportedException(attribute);
                    default: throw new StepException($"Attribute {attributeName} has an unknown data type.");
                }
            }
            catch (InvalidCastException)
            {
                Log.Debug($"Could not cast ({value}) into {attribute.AttributeTypeName.Value}.");
                throw new StepException($"Could not cast ({value}) into {attribute.AttributeTypeName.Value}.");
            }
        }

        private string ConvertString(string value, int maxLength)
        {
            if (TrimLongStrings && value.Length > maxLength)
            {
                return value.Substring(0, maxLength);
            }
            else
            {
                return value;
            }
        }

        private static bool ConvertValueToBooleanAttributeValue(object value, BooleanAttributeMetadata attribute)
        {
            if (value is string label)
            {
                if (label.Equals(attribute.OptionSet.TrueOption.Label.UserLocalizedLabel.Label, StringComparison.InvariantCultureIgnoreCase))
                {
                    return true;
                }
                else if (label.Equals(attribute.OptionSet.FalseOption.Label.UserLocalizedLabel.Label, StringComparison.InvariantCultureIgnoreCase))
                {
                    return false;
                }
                else
                {
                    throw new StepException($"'{label}' is not a valid option for {attribute.LogicalName}.");
                }
            }
            else
            {
                int optionValue = 0;
                try
                {
                    optionValue = ((IConvertible)value).ToInt32(System.Globalization.CultureInfo.InvariantCulture);
                }
                catch (Exception)
                {
                    throw new StepException($"Cannot convert {value} ({value.GetType()}) into an integer.");
                }
                if (optionValue == attribute.OptionSet.TrueOption.Value)
                {
                    return true;
                }
                else if (optionValue == attribute.OptionSet.FalseOption.Value)
                {
                    return false;
                }
                else
                {
                    throw new StepException($"{optionValue} is not a valid option for {attribute.LogicalName}.");
                }
            }
        }

        private static OptionSetValue ConvertValueToEnumAttributeValue(object value, EnumAttributeMetadata attribute)
        {
            return (value is string label) ? ConvertStringToOptionSetValue(label, attribute) :
                                        new OptionSetValue(((IConvertible)value).ToInt32(System.Globalization.CultureInfo.InvariantCulture));
        }

        private static OptionSetValueCollection ConvertToMultiSelectPickListValue(object value, MultiSelectPicklistAttributeMetadata attribute)
        {
            var osValue = ConvertValueToEnumAttributeValue(value, attribute);
            if (osValue == null) return null;
            var collection = new OptionSetValueCollection
            {
                osValue
            };
            return collection;
        }

        private static OptionSetValue ConvertStringToOptionSetValue(string label, EnumAttributeMetadata attribute)
        {
            if (string.IsNullOrEmpty(label)) return null;
            var option = attribute.OptionSet.Options.FirstOrDefault(o => o.Label.UserLocalizedLabel.Label == label);
            if (option == null) throw new StepException($"No option set value with label '{label}' exists.");
            return new OptionSetValue(option.Value.Value);
        }
    }
}
