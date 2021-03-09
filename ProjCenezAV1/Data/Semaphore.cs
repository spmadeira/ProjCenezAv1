using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace ProjCenezAV1.Data
{
    public class Semaphore : INotifyCompletion
    {
        #region Awaiter Impl

        public TaskAwaiter GetAwaiter() => CompositeTask;
        
        public bool IsCompleted => CompositeTask.IsCompleted;

        public void GetResult() => CompositeTask.GetResult();
        
        public void OnCompleted(Action continuation)
        {
            continuation();
        }
        
        #endregion
        
        private TaskAwaiter CompositeTask { get; }

        public Semaphore(IEnumerable<ParallelTask> taskInitializers, int threadCount)
        {
            var taskQueues = Enumerable
                .Range(0, threadCount)
                .Select(_ => new Queue<ParallelTask>()).ToArray();

            using var enumerator = taskInitializers.GetEnumerator();

            var queueIndex = 0;
            
            while (enumerator.MoveNext())
            {
                var queue = taskQueues[queueIndex];
                queue.Enqueue(enumerator.Current);

                queueIndex++;
                if (queueIndex >= threadCount)
                    queueIndex = 0;
            }
            
            enumerator.Dispose();

            var mtx = new Mutex();

            var tasks = taskQueues
                .Select(q => Task.Run(() =>
                {
                    while (q.Count > 0)
                    {
                        var nextTask = q.Dequeue();
                        nextTask.NonCriticalSection();
                        Print($"Thread {Thread.GetCurrentProcessorId()} querendo acessar.");
                        mtx.WaitOne();
                        Print($"Thread {Thread.GetCurrentProcessorId()} acessou.");
                        try
                        {
                            nextTask.CriticalSection();
                        }
                        finally
                        {
                            mtx.ReleaseMutex();
                        }
                        Print($"Thread {Thread.GetCurrentProcessorId()} liberou.");
                    }
                }));

            CompositeTask = Task.WhenAll(tasks).GetAwaiter();
        }

        [Conditional("DEBUG")] 
        private void Print(string str) => Console.WriteLine(str);
    }
}