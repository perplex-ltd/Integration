using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
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
    public class EstablishCrmOptionSetValues : AbstractCrmStep, IDataSink, IDataSource
    {
        public IPipelineInput Input { get; set; }
        public IPipelineOutput Output { get; set; }

        [Property(Required = true)]
        public string EntityLogicalName { get; set; }
        [Property(Required = true)]
        public string AttributeLogicalName { get; set; }
        [Property(Description = "Specifies the Option Value prefix to use when creating new option values. " +
            "Set this to the value of the publisher you use.")]
        public int OptionValuePrefix { get; set; } = 0;
        [Property(Required = false,
            Description = "Specifies the field name that contains the label to be added to the option set.",
            Default = "label")]
        public string LabelKey { get; set; } = "label";
        [Property(Description = "The langage code to use when creating new option set values.",
            Default = 1033)]
        public int LanguageCode { get; set; } = 1033;

        private Dictionary<int, string> options;
        private bool isGlobalOptionSet;
        private string optionSetName;

        public override void Initialise()
        {
            base.Initialise();
            var am = CrmServiceClient.GetEntityAttributeMetadataForAttribute(
                EntityLogicalName, AttributeLogicalName);
            if (am is EnumAttributeMetadata enumAttribute)
            {
                isGlobalOptionSet = enumAttribute.OptionSet.IsGlobal.Value;
                optionSetName = enumAttribute.OptionSet.Name;
                options = new Dictionary<int, string>();
                foreach (var option in enumAttribute.OptionSet.Options)
                {
                    options.Add(option.Value.Value, option.Label.UserLocalizedLabel.Label);
                }
            }
            else
            {
                throw new StepException($"Attribute {AttributeLogicalName} is not a picklist.");
            }
        }
        public override void Execute()
        {
            bool needsPublishing = false;
            while (Input.HasRowsAvailable)
            {
                var row = Input.RemoveRow();
                var label = row[LabelKey] as string;
                if (string.IsNullOrEmpty(label)) continue;
                if (!options.ContainsValue(label))
                {
                    // create option set value
                    int? optionValue = null;
                    if (OptionValuePrefix != 0)
                    {
                        optionValue = GetFirstAvailableOptionSetValue();
                    }
                    optionValue = InsertOptionSetValue(label, optionValue);
                    options.Add(optionValue.Value, label);
                    row[LabelKey + "$value"] = optionValue;
                    Output.AddRow(row);
                    needsPublishing = true;
                } 
                else
                {
                    // add existing option set value to output
                    var optionValue = options.FirstOrDefault(kvp => kvp.Value == label).Key;
                    row[LabelKey + "$value"] = optionValue;
                }
            }
            if (needsPublishing)
            {
                if (isGlobalOptionSet)
                {
                    Log.Information("Publishing option set {optionSetName}", optionSetName);
                    CrmServiceClient.Execute(new PublishXmlRequest()
                    {
                        ParameterXml =
$@"<importexportxml>
 <optionsets>
  <optionset>{optionSetName}</optionset>
 </optionsets>
</importexportxml>"
                    });
                }
                else
                {
                    Log.Information("Publishing entity {EntityLogicalName}", EntityLogicalName);
                    CrmServiceClient.PublishEntity(EntityLogicalName);
                }
            }
        }

        private int InsertOptionSetValue(string label, int? value)
        {
            var request = new InsertOptionValueRequest
            {
                Label = new Label(label, LanguageCode)
            };
            if (value.HasValue)
            {
                request.Value = value;
            }
            if (isGlobalOptionSet)
            {
                request.OptionSetName = optionSetName;
            }
            else
            {
                request.EntityLogicalName = EntityLogicalName;
                request.AttributeLogicalName = AttributeLogicalName;
            }
            Log.Debug("Adding option '{label}'", label);
            var response = (InsertOptionValueResponse)CrmServiceClient.Execute(request);
            return response.NewOptionValue;
        }

        private int GetFirstAvailableOptionSetValue()
        {
            int value = OptionValuePrefix * 10000;
            while (options.ContainsKey(value))
            {
                value++;
            }
            return value;
        }
    }
}
