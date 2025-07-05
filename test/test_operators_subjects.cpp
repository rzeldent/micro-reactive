#include <iostream>
#include <vector>
#include <functional>
#include <memory>
#include <string>
#include <exception>

// Test our micro-reactive library 
#include "micro-reactive.h"

// Simple assertion macro for testing
#define ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "ASSERTION FAILED: " << message << " at line " << __LINE__ << std::endl; \
            return false; \
        } \
    } while(0)

#define ASSERT_EQ(expected, actual, message) \
    do { \
        if ((expected) != (actual)) { \
            std::cerr << "ASSERTION FAILED: " << message << " - expected " << (expected) << " but got " << (actual) << " at line " << __LINE__ << std::endl; \
            return false; \
        } \
    } while(0)

// Test result collector
class TestObserver : public rx::IObserver<int> {
public:
    std::vector<int> values;
    bool completed = false;
    bool error = false;
    std::string error_message;

    void OnNext(const int& value) override {
        values.push_back(value);
    }

    void OnCompleted() override {
        completed = true;
    }

    void OnError(const std::exception& e) override {
        error = true;
        error_message = e.what();
    }

    void reset() {
        values.clear();
        completed = false;
        error = false;
        error_message.clear();
    }
};

// Test Map operator
bool test_map_operator() {
    std::cout << "  Testing Map operator..." << std::endl;
    
    auto source = rx::Range(1, 5, 1);
    std::shared_ptr<rx::IObservable<int>> observable = source;
    auto mapped = rx::Map<int, int>(observable, [](const int& x) { return x * 2; });
    
    auto observer = std::make_shared<TestObserver>();
    mapped->Subscribe(observer);
    
    ASSERT_EQ(5, observer->values.size(), "Map should emit 5 values");
    ASSERT_EQ(2, observer->values[0], "First mapped value should be 2");
    ASSERT_EQ(4, observer->values[1], "Second mapped value should be 4");
    ASSERT_EQ(6, observer->values[2], "Third mapped value should be 6");
    ASSERT_EQ(8, observer->values[3], "Fourth mapped value should be 8");
    ASSERT_EQ(10, observer->values[4], "Fifth mapped value should be 10");
    ASSERT(observer->completed, "Map should complete");
    
    return true;
}

// Test Filter operator
bool test_filter_operator() {
    std::cout << "  Testing Filter operator..." << std::endl;
    
    auto source = rx::Range(1, 5, 1);
    std::shared_ptr<rx::IObservable<int>> observable = source;
    auto filtered = rx::Filter(observable, [](const int& x) { return x % 2 == 0; });
    
    auto observer = std::make_shared<TestObserver>();
    filtered->Subscribe(observer);
    
    ASSERT_EQ(2, observer->values.size(), "Filter should emit 2 values");
    ASSERT_EQ(2, observer->values[0], "First filtered value should be 2");
    ASSERT_EQ(4, observer->values[1], "Second filtered value should be 4");
    ASSERT(observer->completed, "Filter should complete");
    
    return true;
}

// Test Take operator
bool test_take_operator() {
    std::cout << "  Testing Take operator..." << std::endl;
    
    auto source = rx::Range(1, 10, 1);
    std::shared_ptr<rx::IObservable<int>> observable = source;
    auto taken = rx::Take(observable, 3);
    
    auto observer = std::make_shared<TestObserver>();
    taken->Subscribe(observer);
    
    ASSERT_EQ(3, observer->values.size(), "Take should emit 3 values");
    ASSERT_EQ(1, observer->values[0], "First taken value should be 1");
    ASSERT_EQ(2, observer->values[1], "Second taken value should be 2");
    ASSERT_EQ(3, observer->values[2], "Third taken value should be 3");
    ASSERT(observer->completed, "Take should complete");
    
    return true;
}

// Test Skip operator
bool test_skip_operator() {
    std::cout << "  Testing Skip operator..." << std::endl;
    
    auto source = rx::Range(1, 5, 1);
    std::shared_ptr<rx::IObservable<int>> observable = source;
    auto skipped = rx::Skip(observable, 2);
    
    auto observer = std::make_shared<TestObserver>();
    skipped->Subscribe(observer);
    
    ASSERT_EQ(3, observer->values.size(), "Skip should emit 3 values");
    ASSERT_EQ(3, observer->values[0], "First skipped value should be 3");
    ASSERT_EQ(4, observer->values[1], "Second skipped value should be 4");
    ASSERT_EQ(5, observer->values[2], "Third skipped value should be 5");
    ASSERT(observer->completed, "Skip should complete");
    
    return true;
}

// Test BufferCount operator
bool test_buffercount_operator() {
    std::cout << "  Testing BufferCount operator..." << std::endl;
    
    auto source = rx::Range(1, 6, 1);
    std::shared_ptr<rx::IObservable<int>> observable = source;
    auto buffered = rx::BufferCount<int>(observable, 2);
    
    class VectorObserver : public rx::IObserver<std::vector<int>> {
    public:
        std::vector<std::vector<int>> buffers;
        bool completed = false;
        
        void OnNext(const std::vector<int>& value) override {
            buffers.push_back(value);
        }
        
        void OnCompleted() override {
            completed = true;
        }
        
        void OnError(const std::exception& e) override {}
    };
    
    auto observer = std::make_shared<VectorObserver>();
    buffered->Subscribe(observer);
    
    ASSERT_EQ(3, observer->buffers.size(), "BufferCount should emit 3 buffers");
    ASSERT_EQ(2, observer->buffers[0].size(), "First buffer should have 2 items");
    ASSERT_EQ(1, observer->buffers[0][0], "First buffer first item should be 1");
    ASSERT_EQ(2, observer->buffers[0][1], "First buffer second item should be 2");
    ASSERT_EQ(2, observer->buffers[1].size(), "Second buffer should have 2 items");
    ASSERT_EQ(3, observer->buffers[1][0], "Second buffer first item should be 3");
    ASSERT_EQ(4, observer->buffers[1][1], "Second buffer second item should be 4");
    ASSERT_EQ(2, observer->buffers[2].size(), "Third buffer should have 2 items");
    ASSERT_EQ(5, observer->buffers[2][0], "Third buffer first item should be 5");
    ASSERT_EQ(6, observer->buffers[2][1], "Third buffer second item should be 6");
    ASSERT(observer->completed, "BufferCount should complete");
    
    return true;
}

// Test chaining operators
bool test_operator_chaining() {
    std::cout << "  Testing operator chaining..." << std::endl;
    
    auto source = rx::Range(1, 10, 1);
    std::shared_ptr<rx::IObservable<int>> observable = source;
    auto mapped = rx::Map<int, int>(observable, [](const int& x) { return x * 2; });
    std::shared_ptr<rx::IObservable<int>> mappedObs = mapped;
    auto filtered = rx::Filter(mappedObs, [](const int& x) { return x > 5; });
    std::shared_ptr<rx::IObservable<int>> filteredObs = filtered;
    auto taken = rx::Take(filteredObs, 3);
    
    auto observer = std::make_shared<TestObserver>();
    taken->Subscribe(observer);
    
    ASSERT_EQ(3, observer->values.size(), "Chained operators should emit 3 values");
    ASSERT_EQ(6, observer->values[0], "First chained value should be 6");
    ASSERT_EQ(8, observer->values[1], "Second chained value should be 8");
    ASSERT_EQ(10, observer->values[2], "Third chained value should be 10");
    ASSERT(observer->completed, "Chained operators should complete");
    
    return true;
}

// Test Subject
bool test_subject() {
    std::cout << "  Testing Subject..." << std::endl;
    
    auto subject = rx::CreateSubject<int>();
    auto observer = std::make_shared<TestObserver>();
    
    subject->Subscribe(observer);
    
    subject->OnNext(1);
    subject->OnNext(2);
    subject->OnNext(3);
    subject->OnCompleted();
    
    ASSERT_EQ(3, observer->values.size(), "Subject should emit 3 values");
    ASSERT_EQ(1, observer->values[0], "First subject value should be 1");
    ASSERT_EQ(2, observer->values[1], "Second subject value should be 2");
    ASSERT_EQ(3, observer->values[2], "Third subject value should be 3");
    ASSERT(observer->completed, "Subject should complete");
    
    return true;
}

// Test BehaviorSubject
bool test_behavior_subject() {
    std::cout << "  Testing BehaviorSubject..." << std::endl;
    
    auto subject = rx::CreateBehaviorSubject<int>(42);
    auto observer = std::make_shared<TestObserver>();
    
    // Subscribe after setting initial value
    subject->Subscribe(observer);
    
    ASSERT_EQ(1, observer->values.size(), "BehaviorSubject should emit initial value");
    ASSERT_EQ(42, observer->values[0], "Initial value should be 42");
    
    // Add more values
    subject->OnNext(1);
    subject->OnNext(2);
    subject->OnCompleted();
    
    ASSERT_EQ(3, observer->values.size(), "BehaviorSubject should emit 3 values total");
    ASSERT_EQ(42, observer->values[0], "First value should be initial value");
    ASSERT_EQ(1, observer->values[1], "Second value should be 1");
    ASSERT_EQ(2, observer->values[2], "Third value should be 2");
    ASSERT(observer->completed, "BehaviorSubject should complete");
    
    return true;
}

// Test ReplaySubject
bool test_replay_subject() {
    std::cout << "  Testing ReplaySubject..." << std::endl;
    
    auto subject = rx::CreateReplaySubject<int>(2); // Buffer size 2
    
    // Emit values before subscription
    subject->OnNext(1);
    subject->OnNext(2);
    subject->OnNext(3); // This will evict the first value
    
    auto observer = std::make_shared<TestObserver>();
    subject->Subscribe(observer);
    
    ASSERT_EQ(2, observer->values.size(), "ReplaySubject should replay 2 values");
    ASSERT_EQ(2, observer->values[0], "First replayed value should be 2");
    ASSERT_EQ(3, observer->values[1], "Second replayed value should be 3");
    
    // Add more values after subscription
    subject->OnNext(4);
    subject->OnCompleted();
    
    ASSERT_EQ(3, observer->values.size(), "ReplaySubject should emit 3 values total");
    ASSERT_EQ(4, observer->values[2], "New value should be 4");
    ASSERT(observer->completed, "ReplaySubject should complete");
    
    return true;
}

int main() {
    std::cout << "=== Micro-Reactive Library Operators & Subjects Test Suite ===" << std::endl;
    
    std::vector<std::pair<std::string, std::function<bool()>>> tests = {
        {"Map Operator", test_map_operator},
        {"Filter Operator", test_filter_operator},
        {"Take Operator", test_take_operator},
        {"Skip Operator", test_skip_operator},
        {"BufferCount Operator", test_buffercount_operator},
        {"Operator Chaining", test_operator_chaining},
        {"Subject", test_subject},
        {"BehaviorSubject", test_behavior_subject},
        {"ReplaySubject", test_replay_subject}
    };
    
    std::cout << "\nTesting Operators & Subjects:" << std::endl;
    
    int total_tests = 0;
    int passed_tests = 0;
    
    for (const auto& test : tests) {
        std::cout << "Running " << test.first << "..." << std::endl;
        total_tests++;
        
        try {
            if (test.second()) {
                std::cout << "  PASSED" << std::endl;
                passed_tests++;
            } else {
                std::cout << "  FAILED" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cout << "  FAILED with exception: " << e.what() << std::endl;
        } catch (...) {
            std::cout << "  FAILED with unknown exception" << std::endl;
        }
    }
    
    std::cout << "\n=== Test Results ===" << std::endl;
    std::cout << "Tests run: " << total_tests << std::endl;
    std::cout << "Tests passed: " << passed_tests << std::endl;
    std::cout << "Tests failed: " << (total_tests - passed_tests) << std::endl;
    
    if (passed_tests == total_tests) {
        std::cout << "🎉 ALL TESTS PASSED! 🎉" << std::endl;
        return 0;
    } else {
        std::cout << "❌ SOME TESTS FAILED ❌" << std::endl;
        return 1;
    }
}
