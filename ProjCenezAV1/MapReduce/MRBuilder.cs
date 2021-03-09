using System;
using System.Collections.Generic;

namespace ProjCenezAV1.MapReduce
{
    public abstract class MapReduce
    {
        public static MRBuilder<TInput> WithInput<TInput>()
        {
            return new();
        }
    }
    
    public class MRBuilder<TInput>
    {
        public MRBuilder<TInput, TData> WithReader<TData>(Func<TInput, IEnumerable<TData>> readFunction)
        {
            return new()
            {
                Read = readFunction
            };
        }
    }

    public class MRBuilder<TInput, TData> : MRBuilder<TInput>
    {
        public Func<TInput, IEnumerable<TData>> Read { get; init; }

        public MRBuilder<TInput, TData, TKey, TValue> WithMapper<TKey, TValue>(
            Func<TData, IEnumerable<KeyValuePair<TKey, TValue>>> mapper)
        {
            return new()
            {
                Read = Read,
                Map = mapper
            };
        }
        
    }

    public class MRBuilder<TInput, TData, TKey, TValue> : MRBuilder<TInput, TData>
    {
        public static Func<TKey, TKey, bool> DEFAULT_COMPARER 
            = (k1, k2) => k1.Equals(k2);
        public static Action<KeyValuePair<TKey, TValue>> DEFAULT_WRITER 
            = (pair) => Console.WriteLine($"Key: {pair.Key} | Value: {pair.Value}");
        
        internal Func<TData, IEnumerable<KeyValuePair<TKey, TValue>>> Map { get; init; }
        internal Func<TKey, TKey, bool> Compare { get; set; } = DEFAULT_COMPARER;
        internal Func<TKey, IEnumerable<TValue>, TValue> Reduce { get; set; }
        internal Action<KeyValuePair<TKey, TValue>> Write { get; set; } = DEFAULT_WRITER;

        public MRBuilder<TInput, TData, TKey, TValue> WithComparer(Func<TKey, TKey, bool> comparer)
        {
            Compare = comparer;
            return this;
        }
        
        public MRBuilder<TInput, TData, TKey, TValue> WithReducer(Func<TKey, IEnumerable<TValue>, TValue> reducer)
        {
            Reduce = reducer;
            return this;
        }

        public MRBuilder<TInput, TData, TKey, TValue> WithWriter(Action<KeyValuePair<TKey, TValue>> writer)
        {
            Write = writer;
            return this;
        }

        public TMapReduce Build<TMapReduce>() where TMapReduce : IMapReduce<TInput, TData, TKey, TValue>, new()
        {
            if (Reduce == null)
                throw new ArgumentException("Reducer cannot be null", nameof(Reduce));

            var mapReduce = new TMapReduce();
            mapReduce.Build(Read, Map, Compare, Reduce, Write);
            return mapReduce;
        }
    }
}