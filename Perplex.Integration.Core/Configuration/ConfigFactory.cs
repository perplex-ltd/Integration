using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace Perplex.Integration.Core.Configuration
{

    [Serializable()]
    public class InvalidConfigurationException : Exception
    {
        private InvalidConfigurationException() { }
        public InvalidConfigurationException(string message) : base(message) { }
        public InvalidConfigurationException(string message, Exception innerException) : base(message, innerException) { }
        public InvalidConfigurationException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class ConfigFactory
    {
        public readonly static ConfigFactory Default = new ConfigFactory();

        private readonly Dictionary<string, Type> steps = new Dictionary<string, Type>();
        private readonly Dictionary<string, string> connectionStrings = new Dictionary<string, string>();

        private void LoadStepsFromAssembly(Assembly assembly)
        {
            foreach (Type step in assembly.GetTypes().Where(t => t.IsClass && t.IsSubclassOf(typeof(JobStep))))
            {
                StepAttribute stepAttribute = step.GetCustomAttribute<StepAttribute>();
                if (stepAttribute != null)
                {
                    var stepName = stepAttribute.Name ?? step.Name;
                    steps.Add(stepName, step);
                }
            }
        }

        public IntegrationConfig LoadFromFile(string fileName)
        {

            var config = new IntegrationConfig();
            var doc = XDocument.Load(fileName);
            var root = doc.Element("integrationConfig");
            // connection strings
            var connectionStringQuery = from e in root.Elements("connectionStrings").Elements("connectionString")
                                        select new { Name = e.Attribute("name")?.Value, ConnectionString = e.Value };
            foreach (var cs in connectionStringQuery)
            {
                connectionStrings.Add(cs.Name, cs.ConnectionString);
            }
            // load assemblies
            LoadStepsFromAssembly(Assembly.GetExecutingAssembly());
            var assemblyQuery = from e in root.Elements("extensions").Elements("assembly")
                                select Assembly.Load(e.Attribute("name")?.Value);
            foreach (var a in assemblyQuery)
            {
                LoadStepsFromAssembly(a);
            }
            // jobs
            foreach (var jobElement in root.Elements("jobs").Elements("job"))
            {
                var jobId = jobElement.Attribute("id")?.Value;
                if (string.IsNullOrEmpty(jobId)) throw new InvalidConfigurationException("No Job Id specified.");
                var job = new Job()
                {
                    Id = jobId
                };
                foreach (var stepElement in jobElement.Elements("step"))
                {
                    var stepId = stepElement.Attribute("id")?.Value;
                    if (string.IsNullOrEmpty(stepId)) throw new InvalidConfigurationException("No step id specified.");
                    var stepType = stepElement.Attribute("type")?.Value;
                    if (string.IsNullOrEmpty(stepType)) throw new InvalidConfigurationException("No step type specified.");
                    if (!steps.ContainsKey(stepType)) throw new InvalidConfigurationException("{0} is not a valid step type.", stepType);
                    var step = (JobStep)Activator.CreateInstance(steps[stepType]);
                    try
                    {
                        PopulateObjectProperties(stepElement, step);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidConfigurationException(
                            string.Format("Invalid configuration for '{0}' step in job '{1}': {2}",
                                string.IsNullOrEmpty(stepId) ? "anonymous" : stepId, jobId, ex.Message),
                            ex);
                    }
                    job.AddStep(step);
                }
                config.Jobs.Add(job.Id, job);
            }
            return config;
        }

        private void PopulateObjectProperties(XElement node, Object obj)
        {
            var propertyQuery = obj.GetType().GetProperties()
                .Select(p => new { Property = p, Attribute = p.GetCustomAttribute<PropertyAttribute>(true) })
                .Where(p => p.Attribute != null);

            // Now, get all properties from config
            foreach (var p in propertyQuery)
            {
                if (p.Attribute is ConnectionStringAttribute)
                {
                    var connectionQuery = from e in node.Elements("connection")
                                          where e.Attribute("name")?.Value == p.Attribute.Name
                                          select e.Value;
                    var connection = connectionQuery.FirstOrDefault();
                    if (string.IsNullOrEmpty(connection)) throw new InvalidConfigurationException("Connection {0} not found.", p.Attribute.Name);
                    if (connectionStrings.ContainsKey(connection))
                    {
                        p.Property.SetValue(obj, connectionStrings[connection]);
                    }
                    else if (p.Attribute.Required)
                    {
                        throw new InvalidConfigurationException(
                            "Required connection property {0} is missing or no connection string defined.",
                            p.Attribute.Name);
                    }
                }
                else
                {
                    var name = p.Attribute.Name;
                    if (string.IsNullOrEmpty(name))
                    {
                        name = p.Property.Name;
                    }
                    name = convertToXmlConfigName(name);
                    // collection property
                    if (IsCollection(p.Property.PropertyType))
                    {
                        dynamic collection = p.Property.GetValue(obj);
                        // find all tags
                        foreach (var element in node.Elements(name))
                        {
                            // convert into property value 
                            var itemType = p.Property.PropertyType.GetGenericArguments().First();
                            // value needs to be dynamic, otherwise calling ICollection<>.Add won't work later...
                            dynamic value = ConvertValueFromElement(element, itemType);
                            // add to collection
                            collection.Add(value);
                        }
                    }
                    else // scalar property
                    {
                        // find value (tag or attribute)
                        var element = (p.Attribute.Inline) ?
                            new XElement(name, node.Attribute(name)?.Value) :
                            node.Element(name);
                        // convert to property value
                        var value = (element == null) ? null :
                            ConvertValueFromElement(element, p.Property.PropertyType);
                        if (value != null)
                        {
                            // set property
                            p.Property.SetValue(obj, value);
                        }
                        else if (p.Attribute.Required)
                        {
                            throw new InvalidConfigurationException(
                                "Required connection property {0} is missing or no connection string defined.",
                                p.Attribute.Name);
                        }
                    }

                }

            }
        }

        /// <summary>
        /// Make the first letter of the name lowercase.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private string convertToXmlConfigName(string name)
        {
            if (string.IsNullOrEmpty(name) || char.IsLower(name, 0))
                return name;
            return char.ToLowerInvariant(name[0]) + name.Substring(1);
        }

        private static bool IsCollection(Type type)
        {
            if (typeof(ICollection).IsAssignableFrom(type)) return true;
            if (type.IsGenericType)
            {
                Type typeArgument = type.GetGenericArguments().First();
                Type genericCollection = typeof(ICollection<>).MakeGenericType(typeArgument);
                return genericCollection.IsAssignableFrom(type);
            }
            return false;
        }

        private object ConvertValueFromElement(XElement element, Type type)
        {
            if (element == null)
                return null;
            else if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                return ConvertValueFromElement(element, type.GetGenericArguments()[0]);
            else if (type.IsEnum)
                return Enum.Parse(type, element.Value);
            else if (type.IsAssignableFrom(typeof(string)))
                return element.Value;
            else if (type.IsAssignableFrom(typeof(bool)))
                return Convert.ToBoolean(element.Value, System.Globalization.CultureInfo.InvariantCulture);
            else if (type.IsAssignableFrom(typeof(int)))
                return Convert.ToInt32(element.Value, System.Globalization.CultureInfo.InvariantCulture);

            else if (type.IsClass)
            {
                object value = Activator.CreateInstance(type);
                PopulateObjectProperties(element, value);
                return value;
            }
            else
            {
                throw new InvalidConfigurationException("Cannot convert type {0}.", type);
            }
        }
    }
}
