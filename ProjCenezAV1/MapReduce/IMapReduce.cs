using System;
using System.Collections.Generic;

namespace ProjCenezAV1.MapReduce
{
    public interface IMapReduce<TInput, TData, TKey, TValue>
    {
        public void Build(
            Func<TInput, IEnumerable<TData>> Read,
            Func<TData, IEnumerable<KeyValuePair<TKey, TValue>>> Map,
            Func<TKey, TKey, bool> Compare,
            Func<TKey, IEnumerable<TValue>, TValue> Reduce,
            Action<KeyValuePair<TKey, TValue>> Write);
    }
}