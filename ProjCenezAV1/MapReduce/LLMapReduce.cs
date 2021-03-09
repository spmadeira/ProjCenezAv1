using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ProjCenezAV1.Data;

namespace ProjCenezAV1.MapReduce
{
    public class LLMapReduce<
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

        private IEnumerable<TData> Data;
        private List<KeyValuePair<TKey, TValue>[]> Groups = new();
        private ThreadSafeDictionary<TKey, List<TValue>> Buckets = new(353);
        private List<KeyValuePair<TKey, TValue>> Pairs = new();

        private int ThreadCount { get; set; }
        
        public async Task RunAsync(TInput input, int threadCount)
        {
            ThreadCount = threadCount;
            
            Data = Read(input);

            await BuildGroups();
            await ShuffleGroups();
            await ReduceBuckets();
            await WritePairs();
        }

        private async Task BuildGroups()
        {
            var tasks = Data
                .Select(d =>
                {
                    KeyValuePair<TKey, TValue>[] Group = new KeyValuePair<TKey, TValue>[0];
                    Action nonCrit = () =>
                    {
                        Group = Map(d).ToArray();
                    };
                    Action crit = () =>
                    {
                        Groups.Add(Group);
                    };
                    return new ParallelTask
                    {
                        NonCriticalSection = nonCrit,
                        CriticalSection = crit
                    };
                });

            await new Semaphore(tasks, ThreadCount);
        }

        private async Task ShuffleGroups()
        {
            var tasks = Groups
                .SelectMany(group =>
                {
                    return group.Select(pair =>
                    {
                        List<TValue> bucketList = new();

                        Action nonCrit = () =>
                        {
                            Buckets.AddOrUpdate(pair.Key, () =>
                            {
                                return new List<TValue> {pair.Value};
                            }, (list) =>
                            {
                                list.Add(pair.Value);
                                return list;
                            });
                        };

                        return new ParallelTask
                        {
                            NonCriticalSection = nonCrit,
                            CriticalSection = () => { }
                        };
                    });
                }).ToArray();

            await new Semaphore(tasks, ThreadCount);
        }

        private async Task ReduceBuckets()
        {
            var tasks = Buckets
                .Select(b =>
                {
                    KeyValuePair<TKey, TValue> pair = new KeyValuePair<TKey, TValue>();

                    Action nonCrit = () =>
                    {
                        var value = Reduce(b.Key, b.Value);
                        pair = new KeyValuePair<TKey, TValue>(b.Key, value);
                    };

                    Action crit = () =>
                    {
                        Pairs.Add(pair);
                    };

                    return new ParallelTask
                    {
                        NonCriticalSection = nonCrit,
                        CriticalSection = crit
                    };
                });

            await new Semaphore(tasks, ThreadCount);
        }

        private async Task WritePairs()
        {
            var tasks = Pairs
                .Select(p =>
                {
                    Action nonCrit = () =>
                    {
                        Write(p);
                    };

                    return new ParallelTask
                    {
                        CriticalSection = () => { },
                        NonCriticalSection = nonCrit
                    };
                });

            await new Semaphore(tasks, ThreadCount);
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