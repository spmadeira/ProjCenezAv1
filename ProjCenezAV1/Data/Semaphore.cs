using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace ProjCenezAV1.Data
{
    //Manages parallel tasks.
    public class Semaphore
    {
        #region Awaiter Impl
        public TaskAwaiter GetAwaiter() => CompositeTask;
     
        #endregion
        
        private TaskAwaiter CompositeTask { get; }
        private Queue<int> WaitingList { get; } = new();

        private bool isFirst = true;

        private Semaphore(IEnumerable<ParallelTask> taskInitializers, int threadCount)
        {
            //Separate tasks into queues for each thread to operate on
            var taskQueues = Enumerable
                .Range(0, threadCount)
                .Select(_ => new Queue<ParallelTask>()).ToArray();
            
            using var enumerator = taskInitializers.GetEnumerator();

            var queueIndex = 0;
            
            //Fill the queues with the tasks
            while (enumerator.MoveNext())
            {
                var queue = taskQueues[queueIndex];
                queue.Enqueue(enumerator.Current);

                queueIndex++;
                if (queueIndex >= threadCount)
                    queueIndex = 0;
            }
            
            enumerator.Dispose();

            var tasks = taskQueues
                .Select((q,i) => Task.Run(async () =>
                {
                    //While thread has available tasks
                    while (q.Count > 0)
                    {
                        //Dequeue next task
                        var nextTask = q.Dequeue();
                        //Perform non-critical section
                        nextTask.NonCriticalSection();
                        
                        await WaitForPermission(i);

                        try
                        {
                            //Perform critical section
                            nextTask.CriticalSection();
                        }
                        finally
                        {
                            //Release access to the critical section
                            SignalRelease();
                        }
                        
                    }
                }));

            CompositeTask = Task.WhenAll(tasks).GetAwaiter();
        }

        private async Task WaitForPermission(int threadId)
        {
            lock (WaitingList)
            {
                WaitingList.Enqueue(threadId);
            }

            while (true)
            {
                lock (WaitingList)
                {
                    if (WaitingList.Peek() == threadId)
                        return;
                }
                await Task.Delay(1);
            }
        }

        private void SignalRelease()
        {
            lock (WaitingList)
            {
                WaitingList.Dequeue();
            }
        }

        public static Semaphore StartFromTasks(IEnumerable<ParallelTask> tasks, int threadCount = 1)
        {
            if (threadCount < 1)
            {
                throw new ArgumentException("Thread Count must be equal to or greater than 1.", nameof(threadCount));
            }

            return new Semaphore(tasks, threadCount);
        }
    }
}