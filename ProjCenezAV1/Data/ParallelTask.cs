using System;

namespace ProjCenezAV1.Data
{
    public readonly struct ParallelTask
    {
        public Action NonCriticalSection { get; init; }
        public Action CriticalSection { get; init; }
    }
}