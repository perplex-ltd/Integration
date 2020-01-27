using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1710:Identifiers should have correct suffix", Justification = "Nope")]
    public class Row : Dictionary<string, object>
    {

        [JsonIgnore]
        public virtual IEnumerable<string> Columns { get => this.Keys; }


        public Row() { }
        private Row(IDictionary<string, object> dictionary)
        {
            foreach (var kvp in dictionary)
            {
                base.Add(kvp.Key, kvp.Value);
            }
        }

        public string ToJson()
        {
            return Newtonsoft.Json.JsonConvert.SerializeObject(this);
        }

        public override string ToString()
        {
            return ToJson();
        }

        public static Row FromJson(string values)
        {
            return new Row(
                Newtonsoft.Json.JsonConvert.DeserializeObject<IDictionary<string, object>>(values)
                );
        }
    }
}
