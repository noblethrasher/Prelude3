using System;
using System.Data;
using System.Xml.Linq;

namespace Prelude
{
    public static class ReaderUtils
    {
        static RetrievedValue<T> GetValue<T>(IDataReader rdr, string name, Func<int, T> get_value)
        {
            var ord = rdr.GetOrdinal(name);

            if (!rdr.IsDBNull(ord))
            {
                try
                {
                    return new RetrievedValue<T>(name, get_value(ord), false);
                }
                catch (Exception ex) when (ex is InvalidCastException)
                {
                    return new RetrievedValue<T>($"Unable to cast value at {name} to {typeof(T).Name}");
                }
            }

            return new RetrievedValue<T>(name, default(T), true);
        }

        public static RetrievedValue<int> GetInt32(this IDataReader rdr, string name) => GetValue(rdr, name, rdr.GetInt32);
        public static RetrievedValue<bool> GetBoolean(this IDataReader rdr, string name) => GetValue(rdr, name, rdr.GetBoolean);


        public static RetrievedValue<string> GetString(this IDataReader rdr, string name) => GetValue(rdr, name, rdr.GetString);
        public static RetrievedValue<Guid> GetGuid(this IDataReader rdr, string name) => GetValue(rdr, name, rdr.GetGuid);
    }

    public abstract class RetrievedValue
    {
        public string Name { get; }

        public RetrievedValue(string name) => Name = name;

        public RetrievedValue()
        {

        }

        public abstract bool IsNull { get; }

        protected abstract object InternalValue { get; }

        public void Apply(XDocument doc, string value_attribute_name = null)
        {
            foreach (var elem in doc.Elements(Name))
                Apply(elem);
        }

        public void Apply(XElement elem, string value_attribute_name = null)
        {
            if (value_attribute_name is null)
                value_attribute_name = "value";

            var val = elem.Attribute(value_attribute_name);

            if (val is null)
                elem.Add(new XAttribute(value_attribute_name, InternalValue));
            else
                val.Value = InternalValue.ToString();

        }
    }

    public sealed class RetrievedValue<T> :  RetrievedValue
    {
        readonly bool is_null;

        public RetrievedValue(string name, T value, bool is_null) : base(name)
        {
            Value = value;
            this.is_null = is_null;
        }

        public override bool IsNull => is_null;

        public RetrievedValue(string error)
        {

        }

        protected override object InternalValue => Value;

        public T Value { get; }
    }
}