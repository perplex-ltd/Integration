using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core
{
    [Serializable]
    public class Row : Dictionary<string, object>
    {

        [JsonIgnore]
        public virtual IEnumerable<string> Columns { get => this.Keys; }


        public Row() { }

        /// <summary>
        /// Creates a row that is a copy of <paramref name="copy"/>.
        /// </summary>
        /// <param name="copy"></param>
        public Row(Row copy)
        {
            if (copy == null) throw new ArgumentNullException(nameof(copy));
            foreach (var kvp in copy)
            {
                base.Add(kvp.Key, kvp.Value);
            }
        }

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
                JsonConvert.DeserializeObject<IDictionary<string, object>>(values)
                );
        }

        protected Row(System.Runtime.Serialization.SerializationInfo serializationInfo, System.Runtime.Serialization.StreamingContext streamingContext)
        {
            throw new NotImplementedException();
        }
    }
}
