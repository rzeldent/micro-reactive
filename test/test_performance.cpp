#include "test_utils.h"
#include "../include/core.h"
#include "../include/performance.h"

// Test memory monitoring
void test_memory_monitoring() {
    auto monitor = std::make_shared<MemoryMonitor>();
    
    // Test initial state
    TEST_ASSERT_TRUE(monitor->GetAllocatedBytes() >= 0);
    TEST_ASSERT_TRUE(monitor->GetPeakBytes() >= 0);
    
    // Test allocation tracking
    size_t initial_bytes = monitor->GetAllocatedBytes();
    
    // Simulate memory allocation
    monitor->RecordAllocation(1024);
    TEST_ASSERT_EQUAL(initial_bytes + 1024, monitor->GetAllocatedBytes());
    
    // Test peak tracking
    TEST_ASSERT_TRUE(monitor->GetPeakBytes() >= monitor->GetAllocatedBytes());
    
    // Test deallocation
    monitor->RecordDeallocation(512);
    TEST_ASSERT_EQUAL(initial_bytes + 512, monitor->GetAllocatedBytes());
}

// Test circular buffer
void test_circular_buffer() {
    const size_t buffer_size = 5;
    CircularBuffer<int> buffer(buffer_size);
    
    // Test initial state
    TEST_ASSERT_TRUE(buffer.IsEmpty());
    TEST_ASSERT_FALSE(buffer.IsFull());
    TEST_ASSERT_EQUAL(0, buffer.Size());
    
    // Test adding elements
    for (int i = 1; i <= 3; ++i) {
        buffer.Push(i);
    }
    
    TEST_ASSERT_FALSE(buffer.IsEmpty());
    TEST_ASSERT_FALSE(buffer.IsFull());
    TEST_ASSERT_EQUAL(3, buffer.Size());
    
    // Test retrieving elements (FIFO)
    int value = 0;
    TEST_ASSERT_TRUE(buffer.Pop(value));
    TEST_ASSERT_EQUAL(1, value);
    TEST_ASSERT_EQUAL(2, buffer.Size());
    
    // Fill buffer to capacity
    buffer.Push(4);
    buffer.Push(5);
    buffer.Push(6); // This should make it full
    
    TEST_ASSERT_TRUE(buffer.IsFull());
    TEST_ASSERT_EQUAL(buffer_size, buffer.Size());
    
    // Test overflow behavior (should overwrite oldest)
    buffer.Push(7);
    TEST_ASSERT_TRUE(buffer.IsFull());
    TEST_ASSERT_EQUAL(buffer_size, buffer.Size());
    
    // Verify oldest element was overwritten
    TEST_ASSERT_TRUE(buffer.Pop(value));
    TEST_ASSERT_EQUAL(3, value); // Should be 3, not 2 (which was overwritten)
}

void test_observable_metrics_basic() {
    ObservableMetrics metrics;
    
    // Initial state
    TEST_ASSERT_EQUAL(0, metrics.GetEmissionsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetSubscriptionsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetErrorsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetCompletionsCount());
    
    // Record some events
    metrics.RecordEmission();
    metrics.RecordEmission();
    metrics.RecordSubscription();
    metrics.RecordError();
    metrics.RecordCompletion();
    
    // Verify counts
    TEST_ASSERT_EQUAL(2, metrics.GetEmissionsCount());
    TEST_ASSERT_EQUAL(1, metrics.GetSubscriptionsCount());
    TEST_ASSERT_EQUAL(1, metrics.GetErrorsCount());
    TEST_ASSERT_EQUAL(1, metrics.GetCompletionsCount());
    
    // Test reset
    metrics.Reset();
    TEST_ASSERT_EQUAL(0, metrics.GetEmissionsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetSubscriptionsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetErrorsCount());
    TEST_ASSERT_EQUAL(0, metrics.GetCompletionsCount());
}

void test_observable_metrics_timing() {
    ObservableMetrics metrics;
    
    // Record some emissions with longer delays to ensure timing
    metrics.RecordEmission();
    delay(30);
    metrics.RecordEmission();
    delay(30);
    metrics.RecordEmission();
    
    // Check timing metrics - use more lenient timing check
    auto elapsed = metrics.GetElapsedTime();
    TEST_ASSERT_TRUE(elapsed.count() >= 50); // At least 50ms should have passed
    
    auto since_last = metrics.GetTimeSinceLastEmission();
    TEST_ASSERT_TRUE(since_last.count() >= 0); // Should be non-negative
    
    auto rate = metrics.GetEmissionRate();
    TEST_ASSERT_TRUE(rate > 0); // Should have some emission rate
}

void test_observable_metrics_summary() {
    ObservableMetrics metrics;
    
    // Record some events
    metrics.RecordEmission();
    metrics.RecordEmission();
    metrics.RecordSubscription();
    metrics.RecordCompletion();
    
    // Get summary string
    auto summary = metrics.GetSummary();
    TEST_ASSERT_TRUE(summary.length() > 0);
    
    // Summary should contain our counts (basic string contains check)
    TEST_ASSERT_TRUE(summary.find("Emissions: 2") != std::string::npos);
    TEST_ASSERT_TRUE(summary.find("Subscriptions: 1") != std::string::npos);
    TEST_ASSERT_TRUE(summary.find("Completions: 1") != std::string::npos);
}
