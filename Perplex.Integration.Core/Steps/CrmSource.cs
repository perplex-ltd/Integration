using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;

namespace Perplex.Integration.Core.Steps
{


    [Step(Description = "Dynamics 365 Source")]
    public class CrmSource : AbstractCrmStep, IDataSource
    {
        [Property(Required = true)]
        public string FetchXml { get; set; }

        [IntegerProperty(MinValue = 1, MaxValue = 5000)]
        public int PageSize { get; set; }
        public IPipelineOutput Output { get; set; }

        public CrmSource()
        {
            PageSize = 5000;
        }


        public override void Initialise()
        {
            base.Initialise();
        }

        public override void Execute()
        {
            var conversion = (FetchXmlToQueryExpressionResponse)CrmServiceClient.Execute(new FetchXmlToQueryExpressionRequest() { FetchXml = FetchXml });
            var query = conversion.Query;
            int recordCount = 0;
            Log.Information("Retrieving records");
            EntityCollection results;
            do
            {
                // Build fetchXml string with the placeholders.
                //string xml = AddPagingToFetchXml(FetchXml, pagingCookie, pageNumber);

                results = CrmServiceClient.RetrieveMultiple(query);
                recordCount += results.Entities.Count;
                Log.Information("Got {recordCount} records", recordCount);
                foreach (var e in results.Entities)
                {
                    Row row = new Row();
                    foreach (var kvp in e.Attributes)
                    {
                        row.Add(kvp.Key, ConvertCrmValue(kvp.Value));
                    }
                    Output.AddRow(row);
                }
                ((QueryExpression)query).PageInfo.PageNumber++;
                ((QueryExpression)query).PageInfo.PagingCookie = results.PagingCookie;
            } while (results.MoreRecords);
        }

        /// <summary>
        /// Converts a CRM value into a regular value type.
        /// </summary>
        /// <remarks>
        /// Quite shamelessly stolen from 
        /// https://github.com/jamesnovak/xrmtb.XrmToolBox.Controls/blob/master/XrmToolBox.Controls/Helper/EntitySerializer.cs.
        /// </remarks>
        /// <param name="value"></param>
        /// <returns></returns>
        private object ConvertCrmValue(object value)
        {
            if (value is AliasedValue)
                return ConvertCrmValue(((AliasedValue)value).Value);
            else if (value is EntityReference)
                return ((EntityReference)value).Id;
            else if (value is EntityReferenceCollection)
            {
                var referencedEntity = "";
                foreach (var er in (EntityReferenceCollection)value)
                {
                    if (string.IsNullOrEmpty(referencedEntity))
                    {
                        referencedEntity = er.LogicalName;
                    }
                    else if (referencedEntity != er.LogicalName)
                    {
                        referencedEntity = "";
                        break;
                    }
                }
                var result = "";
                foreach (var er in (EntityReferenceCollection)value)
                {
                    if (!string.IsNullOrEmpty(result))
                    {
                        result += ",";
                    }
                    if (referencedEntity != "")
                    {
                        result += er.Id.ToString();
                    }
                    else
                    {
                        result += er.LogicalName + ":" + er.Id.ToString();
                    }
                }
                return result;
            }
            else if (value is EntityCollection)
            {
                var result = "";
                if (((EntityCollection)value).Entities.Count > 0)
                {

                    foreach (var entity in ((EntityCollection)value).Entities)
                    {
                        if (result != "")
                        {
                            result += ",";
                        }
                        result += entity.Id.ToString();
                    }
                    result = ((EntityCollection)value).EntityName + ":" + result;
                }
                return result;
            }
            else if (value is OptionSetValue)
                return ((OptionSetValue)value).Value;
            else if (value is OptionSetValueCollection)
                return "[" + string.Join(",", ((OptionSetValueCollection)value).Select(v => v.Value.ToString())) + "]";
            else if (value is Money)
                return ((Money)value).Value;
            else if (value is BooleanManagedProperty)
                return ((BooleanManagedProperty)value).Value;
            else
                return value;
        }

    }
}
