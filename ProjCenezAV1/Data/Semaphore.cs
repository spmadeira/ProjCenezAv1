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

        private int[] level;
        private int[] last_to_enter;

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
            
            level = new int[threadCount];
            last_to_enter = new int[threadCount - 1];

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
                            SignalRelease(i);
                        }
                    }
                }));

            CompositeTask = Task.WhenAll(tasks).GetAwaiter();
        }

        //Peterson's
        private async Task WaitForPermission(int threadId)
        {
            //Validação de ordem em niveis
            for (int i = 0; i < last_to_enter.Length; i++)
            {
                //Marca que a thread está no nivel de validação I
                level[threadId] = i;
                //Marca que a thread foi a ultima a entrar nesse nivel
                last_to_enter[i] = threadId;
                
                //Condição 1: essa thread não foi a ultima a entrar nesse nivel
                Func<bool> cond1 = () => last_to_enter[i] != threadId;
                
                //Condição 2: não existe thread igual ou mais avançada
                Func<bool> cond2 = () =>
                {
                    var val = true;
                    
                    for (int k = 0; k < level.Length; k++)
                    {
                        if (level[k] >= i && k != threadId)
                        {
                            val = false;
                            break;
                        }
                    }
                    return val;
                };
                
                while (true)
                {
                    if (cond1() || cond2())
                        break;
                    else
                        await Task.Yield();
                }
            }
        }

        private void SignalRelease(int threadId)
        {
            level[threadId] = -1;
        }

        public static Semaphore StartFromTasks(IEnumerable<ParallelTask> tasks, int threadCount = 1)
        {
            if (threadCount < 1)
            {
                throw new ArgumentException("Thread Count must be equal to or greater than 1.", nameof(threadCount));
            }

            if (Environment.ProcessorCount < threadCount)
            {
                throw new ArgumentException(
                    $"Thread Count must be less than or equal to the number of physical threads on the CPU. Physical threads: {Environment.ProcessorCount}",
                    nameof(threadCount));
            }

            return new Semaphore(tasks, threadCount);
        }
    }
}