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
            //Set thread count for the operations
            ThreadCount = threadCount;
            
            //Parse input into the data type
            //Eg. 'Deer Bear River\nCar Car River\nDeer Car Bear'->['Deer Bear River', 'Car Car River', 'Deer Car Bear']
            Data = Read(input);

            //Build groups from the read data
            //Eg. 'Deer Bear River'->['Deer', 'Bear', 'River']
            await BuildGroups();
            
            //Associate unique keys within groups
            //Eg. ['Deer', 'Bear', 'River'],['Car', 'Car', 'River']->['Deer'],['Bear'],['Car', 'Car'],['River', 'River]
            await ShuffleGroups();
            
            //Reduce groups
            //Eg. ['Car', 'Car']->['Car': 2]
            await ReduceBuckets();
            
            //Write each finalized pair
            await WritePairs();
            
            //Cleanup
            Data = null;
            Groups.Clear();
            Buckets.Clear();
            Pairs.Clear();
        }

        private async Task BuildGroups()
        {
            //Gets builds the tasks from each piece of data
            var tasks = Data
                .Select(d =>
                {
                    KeyValuePair<TKey, TValue>[] Group = new KeyValuePair<TKey, TValue>[0];
                    
                    //Non-critical action: Mapping the data into a group
                    Action nonCrit = () =>
                    {
                        Group = Map(d).ToArray();
                    };
                    //Critical section: Add mapped group into the list of groups
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

            //Run tasks, managed by the Semaphore
            await Semaphore.StartFromTasks(tasks, ThreadCount);
        }

        private async Task ShuffleGroups()
        {
            //Gets builds the tasks from each piece of data in each group
            var tasks = Groups
                .SelectMany(group =>
                {
                    return group.Select(pair =>
                    {
                        List<TValue> bucketList = new();

                        //Non-critical section: Add value to the bucket dictionary
                        //Section is non-criticial due to the dictionary implementation being thread-safe
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
                            CriticalSection = () => { } //No critical section for operation.
                        };
                    });
                }).ToArray();

            //Run tasks, managed by the Semaphore
            await Semaphore.StartFromTasks(tasks, ThreadCount);
        }

        private async Task ReduceBuckets()
        {
            //Build tasks from each data in each bucket
            var tasks = Buckets
                .Select(b =>
                {
                    KeyValuePair<TKey, TValue> pair = new KeyValuePair<TKey, TValue>();

                    //Non-critical section: apply the reductor function to each value group
                    Action nonCrit = () =>
                    {
                        var value = Reduce(b.Key, b.Value);
                        pair = new KeyValuePair<TKey, TValue>(b.Key, value);
                    };

                    //Critical section: add the reduced group into the key-value pair list.
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

            //Run tasks, managed by the Semaphore
            await Semaphore.StartFromTasks(tasks, ThreadCount);
        }

        private async Task WritePairs()
        {
            //Build tasks from key-value pairs
            var tasks = Pairs
                .Select(p =>
                {
                    //Non-critical section: Write finalized key-value pair.
                    Action nonCrit = () =>
                    {
                        Write(p);
                    };

                    return new ParallelTask
                    {
                        CriticalSection = () => { }, //No critical section for operation.
                        NonCriticalSection = nonCrit
                    };
                });

            await Semaphore.StartFromTasks(tasks, ThreadCount);
        }
        
        //Interface implementation: receive arguments from MRBuilder
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