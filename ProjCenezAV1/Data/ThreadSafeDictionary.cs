using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace ProjCenezAV1.Data
{
    //Extremamente WIP, as unicas funções que funcionam são as usadas em LLMapReduce.cs
    public class ThreadSafeDictionary<TKey,TValue> : IDictionary<TKey, TValue>
    {
        #region Declarations

        private class Bucket : IEnumerable<KeyValuePair<TKey, TValue>>
        {
            public TKey Key { get; set; }
            public TValue Value { get; set; }
            public Bucket Next { get; set; }

            public KeyValuePair<TKey, TValue> GetPair()
            {
                return new (Key, Value);
            }

            public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
            {
                var bucket = this;
                while (bucket != null)
                {
                    yield return bucket.GetPair();
                    bucket = bucket.Next;
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        #endregion

        #region Variables

        private Bucket[] Buckets { get; set; }

        #endregion

        #region Ctor

        public ThreadSafeDictionary()
        {
            Buckets = new Bucket[0];
        }

        public ThreadSafeDictionary(int bucketCount)
        {
            Buckets = new Bucket[bucketCount];
        }

        #endregion

        #region Functions

        private void Rehash(int newCount)
        {
            var pairs = Buckets.SelectMany(p => p.ToArray()).ToArray();
            Buckets = new Bucket[newCount];

            foreach (var pair in pairs)
            {
                InnerAdd(pair.Key, pair.Value);
            }
        }

        private void InnerAdd(TKey key, TValue value)
        {
            var index = GetIndex(key);

            if (Buckets[index] == null)
            {
                Buckets[index] = new()
                {
                    Key = key,
                    Value = value
                };
            }
            else
            {
                var bucket = Buckets[index];
                while (bucket.Next != null)
                    bucket = bucket.Next;
                bucket.Next = new()
                {
                    Key = key,
                    Value = value
                };
            }
        }

        #endregion

        #region Interfaces

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            lock (Buckets)
            {
                return Buckets.SelectMany(b =>
                {
                    if (b == null)
                        return new KeyValuePair<TKey, TValue>[0];
                    else
                        return b.ToArray();
                }).GetEnumerator();  
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            lock (Buckets)
            {
                InnerAdd(item.Key, item.Value);
            }
        }

        public void Clear()
        {
            lock (Buckets)
            {
                Buckets = new Bucket[1];       
            }
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            lock (Buckets)
            {
                return this.ToArray().Contains(item);
            }
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            lock (Buckets)
            {
                var pairs = this.ToArray();
                for (int i = arrayIndex; i < arrayIndex + pairs.Length; i++)
                {
                    array[i] = pairs[i - arrayIndex];
                }
            }
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            throw new System.NotImplementedException();
        }

        public int Count => Buckets.ToArray().Length;
        public bool IsReadOnly => false;
        
        public void Add(TKey key, TValue value)
        {
            lock (Buckets)
            {
                InnerAdd(key, value);
            }
        }

        public bool ContainsKey(TKey key)
        {
            lock (Buckets)
            {
                var index = GetIndex(key);
                var val = Buckets[index]?.Any(b => EqualityComparer<TKey>.Default.Equals(b.Key, key));
                return val.HasValue && val.Value;
            }
        }

        public bool Remove(TKey key)
        {
            throw new System.NotImplementedException();
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            lock (Buckets)
            {
                var index = GetIndex(key);
                var bucket = Buckets[index];

                while (bucket != null)
                {
                    if (EqualityComparer<TKey>.Default.Equals(bucket.Key, key))
                    {
                        value = bucket.Value;
                        return true;
                    }

                    bucket = bucket.Next;
                }

                value = default;
                return false;
            }
        }

        public void AddOrUpdate(TKey key, Func<TValue> add, Func<TValue, TValue> update)
        {
            lock (Buckets)
            {
                if (ContainsKey(key))
                {
                    this[key] = update(this[key]);
                }
                else
                {
                    this[key] = add();
                }
            }
        }
        
        public TValue this[TKey key]
        {
            get
            {
                lock (Buckets)
                {
                    var hasValue = TryGetValue(key, out var value);
                    if (hasValue)
                        return value;
                    throw new KeyNotFoundException();
                }
            }
            set
            {
                lock (Buckets)
                {
                    var index = GetIndex(key);
                    var bucket = Buckets[index];

                    while (bucket != null)
                    {
                        if (EqualityComparer<TKey>.Default.Equals(bucket.Key, key))
                        {
                            bucket.Value = value;
                            return;
                        }

                        bucket = bucket.Next;
                    }
                    
                    InnerAdd(key, value);
                }
            }
        }

        public ICollection<TKey> Keys => Buckets.SelectMany(b => b.Select(kvp => kvp.Key)).ToList();
        public ICollection<TValue> Values => Buckets.SelectMany(b => b.Select(kvp => kvp.Value)).ToList();

        private int GetIndex(TKey obj) => (int) ((uint) obj.GetHashCode() % Buckets.Length);

        #endregion
    }
}