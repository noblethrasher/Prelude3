using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Prelude
{
    public struct ColumnName
    {
        readonly string name;

        public ColumnName(string name) => this.name = name;

        public static implicit operator string(ColumnName cname) => cname.name;
        public static implicit operator ColumnName(string name) => new ColumnName(name);
    }

    public abstract class AList<T> : IReadOnlyList<T>
    {
        public abstract int Count { get; }

        public abstract T this[int index] { get; }

        static AList<T> Create<U>(U xs) where U : IList<T>
        {
            if (xs?.Count > 0)
                return new NonEmptyImpl(xs);
            else
                return new Empty();
        }

        public static implicit operator AList<T> (T[] xs) => Create(xs);
        public static implicit operator AList<T>(List<T> xs) => Create(xs);
        public static implicit operator AList<T>(Stack<T> xs) => Create((from x in xs select x).ToList());
        public static implicit operator AList<T>(Queue<T> xs) => Create((from x in xs select x).ToList());

        public abstract IEnumerator<T> GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public interface NonEmpty
        {
            T Head { get; }
            AList<T> Tail { get; }
        }

        sealed class NonEmptyImpl : AList<T>, NonEmpty
        {
            readonly IList<T> xs;

            public NonEmptyImpl(IList<T> xs) => this.xs = xs;

            public override T this[int index] => xs[index];

            public T Head => xs[0];

            public AList<T> Tail => Create(xs.Skip(1).ToList());

            public override int Count => xs.Count;

            public override IEnumerator<T> GetEnumerator() => xs.GetEnumerator();
        }

        sealed class Empty : AList<T>
        {
            public override T this[int index] => throw new IndexOutOfRangeException();

            public override int Count => 0;

            public override IEnumerator<T> GetEnumerator() => Enumerable.Empty<T>().GetEnumerator();
        }
    }
}