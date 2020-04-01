using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.Integration.Core.Configuration
{
    static class TypeHelper
    {

        /// <summary>
        /// Returns a type object based on its string representation.
        /// </summary>
        /// <param name="typeName">A c# type or a full type name</param>
        /// <exception cref="ArgumentNullException">If typeName is null.</exception>
        /// <exception cref="InvalidConfigurationException">For any other error.</exception>
        /// <returns>Returns a type object or throws an InvalidConfigurationException.</returns>
        public static Type GetTypeFromString(string typeName)
        {
            if (typeName == null) throw new ArgumentNullException(nameof(typeName));
            if (typeName == "string") return typeof(string);
            else if (typeName == "char") return typeof(char);
            else if (typeName == "bool") return typeof(bool);
            else if (typeName == "sbyte") return typeof(sbyte);
            else if (typeName == "short") return typeof(short);
            else if (typeName == "int") return typeof(int);
            else if (typeName == "long") return typeof(long);
            else if (typeName == "byte") return typeof(byte);
            else if (typeName == "ushort") return typeof(ushort);
            else if (typeName == "uint") return typeof(uint);
            else if (typeName == "ulong") return typeof(ulong);
            else if (typeName == "float") return typeof(float);
            else if (typeName == "double") return typeof(double);
            else if (typeName == "decimal") return typeof(decimal);
            else
            {
                try
                {
                    return Type.GetType(typeName, false, true);
                }
                catch (Exception ex)
                {
                    throw new InvalidConfigurationException($"{typeName} is not a valid type.", ex);
                }
            }
        }

        /// <summary>
        /// Converts a string into any of the standard value types.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object ConvertStringTo(string value, Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            try
            {
                if (type == typeof(string)) return value;
                else if (type == typeof(char)) return Convert.ToChar(value, CultureInfo.InvariantCulture);
                else if (type == typeof(bool)) return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                else if (type == typeof(sbyte)) return Convert.ToSByte(value, CultureInfo.InvariantCulture);
                else if (type == typeof(short)) return Convert.ToInt16(value, CultureInfo.InvariantCulture);
                else if (type == typeof(int)) return Convert.ToInt32(value, CultureInfo.InvariantCulture);
                else if (type == typeof(long)) return Convert.ToInt64(value, CultureInfo.InvariantCulture);
                else if (type == typeof(byte)) return Convert.ToByte(value, CultureInfo.InvariantCulture);
                else if (type == typeof(ushort)) return Convert.ToUInt16(value, CultureInfo.InvariantCulture);
                else if (type == typeof(uint)) return Convert.ToUInt32(value, CultureInfo.InvariantCulture);
                else if (type == typeof(ulong)) return Convert.ToUInt64(value, CultureInfo.InvariantCulture);
                else if (type == typeof(float)) return Convert.ToSingle(value, CultureInfo.InvariantCulture);
                else if (type == typeof(double)) return Convert.ToDouble(value, CultureInfo.InvariantCulture);
                else if (type == typeof(decimal)) return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                else throw new NotImplementedException("Type is not implemented");
            }
            catch (Exception ex)
            {
                throw new InvalidConfigurationException($"Cannot convert {value} to {type} ({ex.Message}).", ex);
            }
        }
    }
}
