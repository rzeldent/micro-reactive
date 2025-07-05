#include <iostream>
#include <vector>
#include "micro-reactive.h"

// Simple test framework
#define TEST_ASSERT_TRUE_MESSAGE(condition, message) \
    if (!(condition)) { \
        std::cerr << "FAIL: " << message << std::endl; \
        return false; \
    }

#define TEST_ASSERT_EQUAL_MESSAGE(expected, actual, message) \
    if ((expected) != (actual)) { \
        std::cerr << "FAIL: " << message << " (expected: " << (expected) << ", actual: " << (actual) << ")" << std::endl; \
        return false; \
    }

#define TEST_FAIL_MESSAGE(message) \
    std::cerr << "FAIL: " << message << std::endl; \
    throw std::runtime_error(message);

// Test runner state
int testsRun = 0;
int testsPassed = 0;
int testsFailed = 0;

bool runTest(const std::string& testName, bool (*testFunc)()) {
    std::cout << "Running " << testName << "..." << std::endl;
    testsRun++;
    
    try {
        if (testFunc()) {
            std::cout << "  PASSED" << std::endl;
            testsPassed++;
            return true;
        } else {
            std::cout << "  FAILED" << std::endl;
            testsFailed++;
            return false;
        }
    } catch (const std::exception& e) {
        std::cout << "  FAILED: " << e.what() << std::endl;
        testsFailed++;
        return false;
    }
}

// Test functions
bool Test_Empty() {
    class EmptyTestObserver : public rx::IObserver<int> {
    public:
        bool _onCompleted = false;
        
        void OnNext(const int &value) override {
            TEST_FAIL_MESSAGE("OnNext was called - Empty should not emit values");
        }

        void OnCompleted() override {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("OnError was called - Empty should not error");
        }
    };

    auto observer = std::make_shared<EmptyTestObserver>();
    auto observable = rx::Empty<int>();
    observable->Subscribe(observer);
    
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    return true;
}

bool Test_Never() {
    class NeverTestObserver : public rx::IObserver<int> {
    public:
        void OnNext(const int &value) override {
            TEST_FAIL_MESSAGE("OnNext was called - Never should not emit values");
        }

        void OnCompleted() override {
            TEST_FAIL_MESSAGE("OnCompleted was called - Never should not complete");
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("OnError was called - Never should not error");
        }
    };

    auto observer = std::make_shared<NeverTestObserver>();
    auto observable = rx::Never<int>();
    observable->Subscribe(observer);
    
    // Never should not emit anything, so this test passes if no failures occur
    return true;
}

bool Test_Range() {
    class RangeTestObserver : public rx::IObserver<int> {
    public:
        std::vector<int> receivedValues;
        bool _onCompleted = false;
        
        void OnNext(const int &value) override {
            receivedValues.push_back(value);
        }

        void OnCompleted() override {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("OnError was called - Range should not error");
        }
    };

    auto observer = std::make_shared<RangeTestObserver>();
    auto observable = rx::Range<int>(0, 5, 1);  // 0, 1, 2, 3, 4, 5
    observable->Subscribe(observer);
    
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(6, observer->receivedValues.size(), "Expected 6 values");
    
    for (int i = 0; i <= 5; i++) {
        TEST_ASSERT_EQUAL_MESSAGE(i, observer->receivedValues[i], "Range value mismatch");
    }
    
    return true;
}

bool Test_Create() {
    class CreateTestObserver : public rx::IObserver<int> {
    public:
        std::vector<int> receivedValues;
        bool _onCompleted = false;

        void OnNext(const int &value) override {
            receivedValues.push_back(value);
        }

        void OnCompleted() override {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("OnError was called - Create should not error");
        }
    };

    bool invoked = false;
    auto observable = rx::Create<int>([&](std::shared_ptr<rx::IObserver<int>> observer) {
        invoked = true;
        observer->OnNext(42);
        observer->OnNext(43);
        observer->OnCompleted();
    });

    auto observer = std::make_shared<CreateTestObserver>();
    observable->Subscribe(observer);
    
    TEST_ASSERT_TRUE_MESSAGE(invoked, "Create observable was not invoked");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(2, observer->receivedValues.size(), "Expected 2 values");
    TEST_ASSERT_EQUAL_MESSAGE(42, observer->receivedValues[0], "First value should be 42");
    TEST_ASSERT_EQUAL_MESSAGE(43, observer->receivedValues[1], "Second value should be 43");
    
    return true;
}

bool Test_Iterate() {
    class IterateTestObserver : public rx::IObserver<int> {
    public:
        std::vector<int> receivedValues;
        bool _onCompleted = false;

        void OnNext(const int &value) override {
            receivedValues.push_back(value);
        }

        void OnCompleted() override {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("OnError was called - Iterate should not error");
        }
    };

    std::vector<int> sourceValues = {10, 20, 30, 40};
    auto observer = std::make_shared<IterateTestObserver>();
    auto observable = rx::Iterate<int>(sourceValues);
    observable->Subscribe(observer);
    
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(4, observer->receivedValues.size(), "Expected 4 values");
    TEST_ASSERT_EQUAL_MESSAGE(10, observer->receivedValues[0], "First value should be 10");
    TEST_ASSERT_EQUAL_MESSAGE(20, observer->receivedValues[1], "Second value should be 20");
    TEST_ASSERT_EQUAL_MESSAGE(30, observer->receivedValues[2], "Third value should be 30");
    TEST_ASSERT_EQUAL_MESSAGE(40, observer->receivedValues[3], "Fourth value should be 40");
    
    return true;
}

bool Test_Timer() {
    class TimerTestObserver : public rx::IObserver<unsigned long> {
    public:
        bool _onNext = false;
        bool _onCompleted = false;
        unsigned long _receivedValue = 999;

        void OnNext(const unsigned long &value) override {
            _onNext = true;
            _receivedValue = value;
        }

        void OnCompleted() override {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("OnError was called - Timer should not error");
        }
    };

    auto observer = std::make_shared<TimerTestObserver>();
    auto observable = rx::Timer<unsigned long>(100); // 100ms delay
    observable->Subscribe(observer);
    
    TEST_ASSERT_TRUE_MESSAGE(observer->_onNext, "OnNext was not called");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(0UL, observer->_receivedValue, "Timer should emit 0 as first value");
    
    return true;
}

bool Test_Interval() {
    class IntervalTestObserver : public rx::IObserver<unsigned long> {
    public:
        std::vector<unsigned long> receivedValues;
        bool _onCompleted = false;

        void OnNext(const unsigned long &value) override {
            receivedValues.push_back(value);
        }

        void OnCompleted() override {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("OnError was called - Interval should not error");
        }
    };

    auto observer = std::make_shared<IntervalTestObserver>();
    auto observable = rx::Interval<unsigned long>(100); // 100ms interval
    observable->Subscribe(observer);
    
    // In desktop mode, interval emits 3 values (0, 1, 2) then completes
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(3, observer->receivedValues.size(), "Expected 3 values");
    TEST_ASSERT_EQUAL_MESSAGE(0UL, observer->receivedValues[0], "First value should be 0");
    TEST_ASSERT_EQUAL_MESSAGE(1UL, observer->receivedValues[1], "Second value should be 1");
    TEST_ASSERT_EQUAL_MESSAGE(2UL, observer->receivedValues[2], "Third value should be 2");
    
    return true;
}

bool Test_Defer() {
    class DeferTestObserver : public rx::IObserver<int> {
    public:
        std::vector<int> receivedValues;
        bool _onCompleted = false;

        void OnNext(const int &value) override {
            receivedValues.push_back(value);
        }

        void OnCompleted() override {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("OnError was called - Defer should not error");
        }
    };

    int factoryCallCount = 0;
    auto deferObservable = rx::Defer<int>([&factoryCallCount]() -> std::shared_ptr<rx::IObservable<int>> {
        factoryCallCount++;
        std::vector<int> values = {100, 200};
        return rx::Iterate<int>(values);
    });

    auto observer = std::make_shared<DeferTestObserver>();
    deferObservable->Subscribe(observer);
    
    TEST_ASSERT_EQUAL_MESSAGE(1, factoryCallCount, "Factory should be called once");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(2, observer->receivedValues.size(), "Expected 2 values");
    TEST_ASSERT_EQUAL_MESSAGE(100, observer->receivedValues[0], "First value should be 100");
    TEST_ASSERT_EQUAL_MESSAGE(200, observer->receivedValues[1], "Second value should be 200");
    
    return true;
}

bool Test_Scope() {
    class ScopeTestObserver : public rx::IObserver<int> {
    public:
        std::vector<int> receivedValues;
        bool _onCompleted = false;

        void OnNext(const int &value) override {
            receivedValues.push_back(value);
        }

        void OnCompleted() override {
            _onCompleted = true;
        }

        void OnError(const std::exception &e) override {
            TEST_FAIL_MESSAGE("OnError was called - Scope should not error");
        }
    };

    int resourceCallCount = 0;
    int observableCallCount = 0;
    
    auto scopeObservable = rx::Scope<int>(
        [&resourceCallCount]() -> std::vector<int> {
            resourceCallCount++;
            return {10, 20, 30};
        },
        [&observableCallCount](std::vector<int> resource) -> std::shared_ptr<rx::IObservable<int>> {
            observableCallCount++;
            return rx::Iterate<int>(resource);
        }
    );

    auto observer = std::make_shared<ScopeTestObserver>();
    scopeObservable->Subscribe(observer);
    
    TEST_ASSERT_EQUAL_MESSAGE(1, resourceCallCount, "Resource factory should be called once");
    TEST_ASSERT_EQUAL_MESSAGE(1, observableCallCount, "Observable factory should be called once");
    TEST_ASSERT_TRUE_MESSAGE(observer->_onCompleted, "OnCompleted was not called");
    TEST_ASSERT_EQUAL_MESSAGE(3, observer->receivedValues.size(), "Expected 3 values");
    TEST_ASSERT_EQUAL_MESSAGE(10, observer->receivedValues[0], "First value should be 10");
    TEST_ASSERT_EQUAL_MESSAGE(20, observer->receivedValues[1], "Second value should be 20");
    TEST_ASSERT_EQUAL_MESSAGE(30, observer->receivedValues[2], "Third value should be 30");
    
    return true;
}

int main() {
    std::cout << "=== Micro-Reactive Library Comprehensive Test Suite ===" << std::endl;
    std::cout << std::endl;
    
    // Run all source tests
    std::cout << "Testing Source Observables:" << std::endl;
    runTest("Empty", Test_Empty);
    runTest("Never", Test_Never);
    runTest("Range", Test_Range);
    runTest("Create", Test_Create);
    runTest("Iterate", Test_Iterate);
    runTest("Timer", Test_Timer);
    runTest("Interval", Test_Interval);
    runTest("Defer", Test_Defer);
    runTest("Scope", Test_Scope);
    
    std::cout << std::endl;
    std::cout << "=== Test Results ===" << std::endl;
    std::cout << "Tests run: " << testsRun << std::endl;
    std::cout << "Tests passed: " << testsPassed << std::endl;
    std::cout << "Tests failed: " << testsFailed << std::endl;
    
    if (testsFailed == 0) {
        std::cout << "🎉 ALL TESTS PASSED! 🎉" << std::endl;
        return 0;
    } else {
        std::cout << "❌ " << testsFailed << " test(s) failed." << std::endl;
        return 1;
    }
}
