using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using Perplex.Integration.Core.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{
    [Step]
    public class CrmMergeDuplicates : AbstractCrmBatchRequestStep
    {
        [Property("Field", Required = true)]
        public IList<string> MergeFields { get; } = new List<string>();


        
        protected override OrganizationRequest CreateRequest(Row row)
        {
            if (row is null) throw new ArgumentNullException(nameof(row));
            var masterId = (Guid)row["master$" + PrimaryKeyField];
            var subordinateId = (Guid)row["subordinate$" + PrimaryKeyField];
            return CreateMergeRequest(masterId, subordinateId);
        }


        protected override void ProcessReponse(OrganizationResponse response, Row processedRow, OrganizationRequest _)
        {
            if (response is MergeResponse)
            {
                Output.AddRow(processedRow);
            } 
            else
            {
                Log.Error("Response {response} is not a MergeResponse.", response);
                throw new StepException("Unexpected response received.");
            }
            
        }



        private MergeRequest CreateMergeRequest(Guid masterRowId, Guid subordinateRowId)
        {
            // prepare merge request
            var mergeRequest = new MergeRequest()
            {
                Target = new EntityReference(EntityLogicalName, masterRowId),
                SubordinateId = subordinateRowId,
                PerformParentingChecks = false,
                UpdateContent = new Entity(EntityLogicalName)
            };
            // Copy content from subordinate to master, if it's null in the master...
            if (MergeFields.Count > 0)
            {
                var columns = new ColumnSet(MergeFields.ToArray());
                Log.Verbose("Retrieving master record {masterRowId}", masterRowId);
                Entity master = CrmServiceClient.Retrieve(EntityLogicalName, masterRowId, columns);
                Log.Verbose("Retrieving subordinate record {subordinateRowId}", subordinateRowId);
                Entity subordinate = CrmServiceClient.Retrieve(EntityLogicalName, subordinateRowId, columns);
                foreach (var a in subordinate.Attributes.Where(a => a.Key != PrimaryKeyField))
                {
                    var attribute = GetAttributeMetadata(a.Key);
                    if (attribute.IsValidForUpdate.GetValueOrDefault(false))
                    {
                        if (attribute is EnumAttributeMetadata enumAttribute)
                        {
                            // use subordinate value is it's not the default value and if it's not in the master, or the master uses the default value.
                            if ((a.Value as OptionSetValue)?.Value != enumAttribute.DefaultFormValue)
                            {
                                if (!master.Contains(a.Key) || ((master[a.Key] as OptionSetValue)?.Value == enumAttribute.DefaultFormValue))
                                {
                                    mergeRequest.UpdateContent[a.Key] = a.Value;
                                }
                            }
                        }
                        else if (attribute is BooleanAttributeMetadata boolAttribute)
                        {
                            if ((bool)a.Value != boolAttribute.DefaultValue)
                            {
                                if (!master.Contains(a.Key) || ((bool)master[a.Key] == boolAttribute.DefaultValue))
                                {
                                    mergeRequest.UpdateContent[a.Key] = a.Value;
                                }
                            }
                        }
                        else
                        {
                            // use subordinate value if it's not in the master record
                            if (!master.Contains(a.Key))
                            {
                                var value = a.Value;
                                if (value is EntityReference er)
                                {
                                    // Need to re-create entity reference, otherwise the merge request will fail
                                    value = new EntityReference(er.LogicalName, er.Id);
                                }
                                mergeRequest.UpdateContent[a.Key] = value;
                            }
                        }
                    }
                }
            }
            return mergeRequest;
        }

    }
}
