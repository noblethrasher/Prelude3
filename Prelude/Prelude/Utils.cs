using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace System.Runtime.CompilerServices
{
    public sealed class TupleElementNamesAttribute : Attribute
    {
        public IList<string> TransformNames { get; }

        public TupleElementNamesAttribute(string[] names) => TransformNames = names;
    }
}


namespace System
{
    public struct ValueTuple<T1, T2>
    {
        public readonly T1 Item1;
        public readonly T2 Item2;

        public ValueTuple(T1 item1, T2 item2)
        {
            Item1 = item1;
            Item2 = item2;
        }
    }

    public struct ValueTuple<T1, T2, T3>
    {
        public readonly T1 Item1;
        public readonly T2 Item2;
        public readonly T3 Item3;        

        public ValueTuple(T1 item1, T2 item2, T3 item3)
        {
            Item1 = item1;
            Item2 = item2;
            Item3 = item3;
        }
    }
}

namespace Prelude
{
    static class Utils
    {
        public static void Add<TKey, TValue>(this IDictionary<TKey, TValue> memo, KeyValuePair<TKey, TValue> kv)
        {
            memo.Add(kv.Key, kv.Value);
        }
    }
}
