using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace ProjCenezAV1.MapReduce
{
    public class PLinqMapReduce<
        TInput,
        TData,
        TKey,
        TValue> : IMapReduce<TInput, TData, TKey, TValue>
    {
        internal Func<TInput, IEnumerable<TData>> Read { get; set; }
        internal Func<TData, IEnumerable<KeyValuePair<TKey, TValue>>> Map { get; set; }
        internal Func<TKey, TKey, bool> Compare { get; set; }
        internal Func<TKey, IEnumerable<TValue>, TValue> Reduce { get; set; }
        internal Action<KeyValuePair<TKey, TValue>> Write { get; set; }
        
        private ConcurrentBag<IEnumerable<KeyValuePair<TKey, TValue>>> Groups = new();
        private ConcurrentDictionary<TKey, ConcurrentBag<TValue>> Buckets = new();
        private ConcurrentBag<KeyValuePair<TKey, TValue>> Pairs = new();

        public void Run(TInput input)
        {
            var readData = Read(input);
            
            readData
                .AsParallel()
                .ForAll(MapData);
            
            Groups
                .AsParallel()
                .ForAll(Shuffle);
            
            Buckets
                .AsParallel()
                .ForAll(ReduceBucket);
            
            Pairs
                .AsParallel()
                .ForAll(Write);
            
            Cleanup();
        }

        private void MapData(TData data)
        {
            var group = Map(data);
            Groups.Add(group);
        }
        
        private void Shuffle(IEnumerable<KeyValuePair<TKey, TValue>> group)
        {
            group
                .AsParallel()
                .ForAll(pair =>
                {
                    Buckets.AddOrUpdate(pair.Key, (_) => new ConcurrentBag<TValue> {pair.Value}, (_, val) =>
                    {
                        val.Add(pair.Value);
                        return val;
                    });
                });
        }

        private void ReduceBucket(KeyValuePair<TKey, ConcurrentBag<TValue>> bucket)
        {
            var value = Reduce(bucket.Key, bucket.Value.ToArray());
            Pairs.Add(new KeyValuePair<TKey, TValue>(bucket.Key, value));
        }

        private void Cleanup()
        {
            Groups.Clear();
            Buckets.Clear();
            Pairs.Clear();
        }

        public void Build(
            Func<TInput, IEnumerable<TData>> Read, 
            Func<TData, IEnumerable<KeyValuePair<TKey, TValue>>> Map, 
            Func<TKey, TKey, bool> Compare, 
            Func<TKey, IEnumerable<TValue>, TValue> Reduce, 
            Action<KeyValuePair<TKey, TValue>> Write)
        {
            this.Read = Read;
            this.Map = Map;
            this.Compare = Compare;
            this.Reduce = Reduce;
            this.Write = Write;
        }
    }
}