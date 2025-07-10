#include "../include/performance.h"

namespace rx
{
    // Static variable definitions for MemoryMonitor
    std::atomic<size_t> MemoryMonitor::allocated_bytes_{0};
    std::atomic<size_t> MemoryMonitor::peak_bytes_{0};
    std::atomic<size_t> MemoryMonitor::allocation_count_{0};
}
