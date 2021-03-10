using System;

namespace ProjCenezAV1.Data
{
    //Defines a ParallelTask to be executed by the semaphore
    public readonly struct ParallelTask
    {
        //Action representing the non-critical section
        public Action NonCriticalSection { get; init; }
        //Action representing the critical section
        public Action CriticalSection { get; init; }
    }
}