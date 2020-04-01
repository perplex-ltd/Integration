using Perplex.Integration.Core.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Steps
{

    public class ColumnMapping
    {
        public ColumnMapping()
        {
            Mappings = new List<FieldRenameMapping>();
        }

        [Property(Inline = true, Required = true)]
        public string Column { get; set; }
        [Property(Inline = true)]
        public string TargetType
        {
            get { return TargetTypeAsType?.ToString(); }
            set { TargetTypeAsType = TypeHelper.GetTypeFromString(value); }
        }
        [Property("mapping")]
        public IList<FieldRenameMapping> Mappings { get; private set; }

        public Type TargetTypeAsType { get; private set; }

    }




    [Configuration.Step("Field Value Mapper")]
    public class MapFieldValues : DataProcessStep
    {
        public MapFieldValues()
        {
            ColumnMappings = new List<ColumnMapping>();
        }
        [Property("mappings")]
        public IList<ColumnMapping> ColumnMappings { get; private set; }

        readonly Dictionary<string, Dictionary<string, object>> mappings = new Dictionary<string, Dictionary<string, object>>();

        /// <summary>
        /// Converts all Mapping.To values to the correct type.
        /// </summary>
        public override void Initialise()
        {
            base.Initialise();
            foreach (var columnMapping in ColumnMappings)
            {
                var valueMap = new Dictionary<string, object>();
                foreach (var mapping in columnMapping.Mappings)
                {
                    valueMap.Add(mapping.From,
                        TypeHelper.ConvertStringTo((string)mapping.To, columnMapping.TargetTypeAsType));
                }
                mappings.Add(columnMapping.Column, valueMap);
            }
        }

        /// <summary>
        /// Maps all values for all columns specified in ColumnMappings.
        /// </summary>
        /// <remarks>
        /// Null values are removed. Values not specified in Mappings are removed.
        /// </remarks>
        public override void Execute()
        {
            while (Input.HasRowsAvailable)
            {
                var row = Input.RemoveRow();
                foreach (var columnMapping in ColumnMappings)
                {
                    if (row.ContainsKey(columnMapping.Column))
                    {
                        var originalValue = row[columnMapping.Column];
                        var originalValueAsString = originalValue?.ToString();
                        row.Remove(columnMapping.Column);
                        if ( mappings[columnMapping.Column].ContainsKey(originalValueAsString))
                        {
                            row.Add(columnMapping.Column, mappings[columnMapping.Column][originalValueAsString]);
                        }
                        
                    }
                }
                Output.AddRow(row);
            }

        }
    }
}
