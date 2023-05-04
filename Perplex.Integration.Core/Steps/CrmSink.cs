using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using Newtonsoft.Json;
using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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
    public class CrmSink : AbstractCrmBatchRequestStep
    {
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
        [Property(Description = "If set to true, only changed fields will be updated. This setting is ignored for insert or delete requests.")]
        public bool CheckForChanges { get; private set; }

        public override void Validate()
        {
            base.Validate();
            if ((!TakeOperationFromRow) && (!Operation.HasValue)) throw new InvalidConfigurationException("Must specify Operation property if TakeOperationFromRow is null.");
            if ((Operation == Steps.Operation.Upsert) && (string.IsNullOrEmpty(AlternateKey))) throw new InvalidConfigurationException("Must specify an AlternateKey if operation is upsert.");
        }
        public override void Initialise()
        {
            base.Initialise();
        }


        protected override void ProcessReponse(OrganizationResponse response, Row processedRow, OrganizationRequest request)
        {
            if (response is null) throw new ArgumentNullException(nameof(response));
            if (processedRow is null) throw new ArgumentNullException(nameof(processedRow));
            switch (response)
            {
                case CreateResponse cr:
                    processedRow[PrimaryKeyField] = cr.id;
                    Output.AddRow(processedRow);
                    break;
                case UpsertResponse usr:
                    processedRow[PrimaryKeyField] = usr.Target.Id;
                    Output.AddRow(processedRow);
                    break;
                case UpdateResponse _:
                    Output.AddRow(processedRow);
                    break;
                case DeleteResponse _:
                    break;
            }
        }

        protected override OrganizationRequest CreateRequest(Row row)
        {
            if (row is null) throw new ArgumentNullException(nameof(row));
            var operation = (TakeOperationFromRow) ?
                (Operation)Enum.Parse(typeof(Operation), (string)row[OperationFieldName], true) :
                Operation.Value;
            // create entity object with alternate key, if it is specified.
            Entity entity;
            
            if (string.IsNullOrEmpty(AlternateKey))
            {
                entity = new Entity(EntityLogicalName);
            }
            else
            {
                if (!row.ContainsKey(AlternateKey))
                {
                    Log.Debug("Row doesn't contain alternate key {AlternateKey}. {row}", AlternateKey, row);
                    throw new StepException($"Row doesn't contain alternate key {AlternateKey}.");
                }
                entity = new Entity(EntityLogicalName, AlternateKey, GetCrmValue(AlternateKey, row[AlternateKey], null));
            }
            // add primary key field (e.g. contactid) if it exists in row
            if (row.ContainsKey(PrimaryKeyField) && row[PrimaryKeyField] != null && !(row[PrimaryKeyField] is DBNull))
            {
                // parse id
                var id = row[PrimaryKeyField];
                switch (id)
                {
                    case Guid idAsGuid: entity.Id = idAsGuid; break;
                    case string idAsString:
                        try
                        {
                            entity.Id = new Guid(idAsString); break;
                        }
                        catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                        {
                            throw new StepException($"'{idAsString}' in field '{PrimaryKeyField}' is not a valid GUID.");
                        }
                    default:
                        throw new StepException($"Cannot convert the field {PrimaryKeyField} into a GUID.");
                }
            }
            // Is it Insert or Update?
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
            // add all attributes apart from primary key, alternate key and special attributes
            foreach (string column in row.Columns.Where(
                c => (c != AlternateKey) && (c != PrimaryKeyField) && !c.Contains("$")))
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
            if (CheckForChanges)
            {
                // remove unchanged fields
                if (operation == Steps.Operation.Update || operation == Steps.Operation.Upsert)
                {
                    // retrieve existing entity by Id (if known) or by alternative keys
                    Entity existingEntity = RetrieveEntity(entity);

                    if (existingEntity != null)
                    {
                        var attributes = entity.Attributes.Select(a => a.Key).ToList();
                        foreach (var a in attributes)
                        {
                            if (AreCrmValuesSame(entity, existingEntity, a))
                            {
                                entity.Attributes.Remove(a);
                            }
                        }
                        if (entity.Attributes.Count == 0)
                        {
                            // remove whole entity if it is completely unchanged...
                            return null;
                        }
                        else { }
                    }
                }
            }
            // Return Insert, Update, or Upsert reqeust. Delete request was returned earlier...
            return operation switch
            {
                Steps.Operation.Insert => new CreateRequest() { Target = entity },
                Steps.Operation.Update => new UpdateRequest() { Target = entity },
                Steps.Operation.Upsert => new UpsertRequest() { Target = entity },
                _ => throw new StepException("Invalid operation specified"),
            };
        }

        private Entity RetrieveEntity(Entity entity)
        {
            var relationshipQueryCollection = new RelationshipQueryCollection();
            var columnSet = new ColumnSet();
            foreach (var a in entity.Attributes)
            {
                columnSet.AddColumn(a.Key);
                if ((a.Value is EntityReference lookup) && (lookup.Id == Guid.Empty) && (lookup.KeyAttributes.Count > 0))
                {
                    var relatedQuery = new QueryExpression(lookup.LogicalName)
                    {
                        ColumnSet = new ColumnSet(lookup.KeyAttributes.Select(k => k.Key).ToArray())
                    };
                    relationshipQueryCollection.Add(GetRelationshipByLookupAttribute(a.Key, lookup.LogicalName), relatedQuery);
                }
            }
            var retrieveRequest = new RetrieveRequest()
            {
                ColumnSet = columnSet,
                Target = (entity.Id != Guid.Empty) ? entity.ToEntityReference() :
                    new EntityReference(entity.LogicalName, entity.KeyAttributes),
                RelatedEntitiesQuery = relationshipQueryCollection
            };
            try
            {
                var response = (RetrieveResponse)CrmServiceClient.Execute(retrieveRequest);
                return response.Entity;
            }
            catch (System.ServiceModel.FaultException<OrganizationServiceFault>)
            {
                return null;
            }
        }

        /// <summary>
        /// Compares an attribute of two entities to each other.
        /// </summary>
        /// <param name="newEntity">The new entity that is about to be uploaded to CRM.</param>
        /// <param name="existingEntity">The entity that is currently in CRM.</param>
        /// <param name="attribute">The name of the attribute.</param>
        /// <returns>true, if they're the same, false if they're different.</returns>
        private bool AreCrmValuesSame(Entity newEntity, Entity existingEntity, string attribute)
        {
            if (newEntity.Contains(attribute) && existingEntity.Contains(attribute))
            {
                var newAttribute = newEntity[attribute];
                if (newAttribute is EntityReference lookup)
                {
                    // Need to resolve alternative key lookups.
                    if (lookup.Id == Guid.Empty && lookup.KeyAttributes.Count > 0)
                    {
                        var related = existingEntity.RelatedEntities[GetRelationshipByLookupAttribute(attribute, lookup.LogicalName)].Entities.FirstOrDefault();
                        var lookupAsEntity = new Entity(lookup.LogicalName);
                        foreach (var a in lookup.KeyAttributes)
                        {
                            lookupAsEntity[a.Key] = a.Value;
                        }
                        foreach (var a in lookup.KeyAttributes)
                        {
                            if (!AreCrmValuesSame(lookupAsEntity, related, a.Key)) return false;
                        }
                        return true;
                        //var newLookup = RetrieveEntity(lookup, new ColumnSet())?.ToEntityReference();
                        //if (newLookup is null)
                        //{
                        //   Log.Warning("Couldn't resolve lookup {KeyAttributes}", lookup.KeyAttributes);
                        //    return false;
                        //}
                        //newEntity[attribute] = newLookup;
                        //lookup = newLookup;
                    }
                    else
                    {
                        var ref2 = existingEntity[attribute] as EntityReference;
                        return (lookup.Id.Equals(ref2?.Id));// Ids are unique, so there's no need to compare the Logical Name
                    }
                }
                else if (newAttribute is OptionSetValueCollection osCollection)
                {
                    var existingValues= (existingEntity[attribute] as OptionSetValueCollection)?.Select(os => os.Value).OrderBy(v => v).ToList();
                    var newValues = osCollection.Select(os => os.Value).OrderBy(v => v).ToList();
                    if (newValues.Count != existingValues.Count) return false;
                    for (int i = 0; i < newValues.Count; i++)
                    {
                        if (newValues[i] != existingValues[i]) return false;
                    }
                    return true;
                }
                else
                {
                    return Object.Equals(existingEntity[attribute], newEntity[attribute]);
                }
            }
            else if (!newEntity.Contains(attribute) && !existingEntity.Contains(attribute))
            {
                // neither entity contains the attribute...
                return true;
            }
            else
            {
                // only one entity contains the attribute. If it's null or empty, it's still considered identical...
                var e = newEntity.Contains(attribute) ? newEntity : existingEntity;
                var value = e[attribute];
                return (value is null || 
                    (value is string valueAsString && string.IsNullOrEmpty(valueAsString)) ||
                    ((value is ICollection valueAsCollection) && valueAsCollection.Count == 0));
            }
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
            var attribute = GetAttributeMetadata(attributeName);
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
                    case AttributeTypeCode.Owner: //throw new StepAttributeTypeNotSupportedException(attribute);
                        return new EntityReference("systemuser", (Guid)value); 
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
                int optionValue;
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
            // Deserialize into an enumerable, if this is a JSON array
            if (value is string valueAsString && new Regex(@"^\[.*\]$").IsMatch(valueAsString))
            {
                value = JsonConvert.DeserializeObject<List<object>>(valueAsString);
            }
            
            if (value is IEnumerable valueAsEnumerable)
            {
                return new OptionSetValueCollection(ConvertEnumerableToOptionSetValues(valueAsEnumerable, attribute).ToList());
            }   
            else
            {
                var osValue = ConvertValueToEnumAttributeValue(value, attribute);
                if (osValue == null) return null;
                var collection = new OptionSetValueCollection
                {
                    osValue
                };
                return collection;
            }
        }

        private static IEnumerable<OptionSetValue> ConvertEnumerableToOptionSetValues(IEnumerable values, EnumAttributeMetadata attribute)
        {
            foreach ( var value in values)
            {
                yield return ConvertValueToEnumAttributeValue(value, attribute);
            }
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
