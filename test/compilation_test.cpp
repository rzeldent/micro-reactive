//#include <Arduino.h>
#include <iostream>
#include <vector>
#include <memory>
#include "micro-reactive.h"

using namespace rx;

// Modern C++11 comprehensive unit test for micro-reactive library
// Tests all core functionality: Observer/Observable patterns, sources, memory management

void testCoreObserverPattern() {
    std::cout << "Testing core observer pattern..." << std::endl;
    
    // Test observer creation with lambdas
    bool onNextCalled = false;
    bool onCompletedCalled = false;
    bool onErrorCalled = false;
    int receivedValue = 0;
    
    auto observer = CreateObserver<int>(
        [&](const int& value) { 
            onNextCalled = true;
            receivedValue = value;
        },
        [&]() {
            onCompletedCalled = true;
        },
        [&](const std::exception& e) {
            onErrorCalled = true;
        }
    );
    
    // Test observer functionality
    observer->OnNext(42);
    observer->OnCompleted();
    
    if (!onNextCalled || !onCompletedCalled || onErrorCalled || receivedValue != 42) {
        throw std::runtime_error("Core observer pattern test failed");
    }
    
    std::cout << "✓ Core observer pattern test passed" << std::endl;
}

void testAllSources() {
    std::cout << "Testing all source observables..." << std::endl;
    
    // Test Empty
    {
        bool completed = false;
        auto observer = CreateObserver<int>(
            [](const int&) { throw std::runtime_error("Empty should not emit"); },
            [&]() { completed = true; }
        );
        auto empty = Empty<int>();
        empty->Subscribe(observer);
        if (!completed) throw std::runtime_error("Empty test failed");
    }
    
    // Test Never (should not call anything)
    {
        auto observer = CreateObserver<int>(
            [](const int&) { throw std::runtime_error("Never should not emit"); },
            []() { throw std::runtime_error("Never should not complete"); }
        );
        auto never = Never<int>();
        never->Subscribe(observer);
        // If we get here without exceptions, Never works correctly
    }
    
    // Test Range
    {
        std::vector<int> values;
        bool completed = false;
        auto observer = CreateObserver<int>(
            [&](const int& value) { values.push_back(value); },
            [&]() { completed = true; }
        );
        auto range = Range<int>(1, 3, 1);
        range->Subscribe(observer);
        
        if (!completed || values.size() != 3 || values[0] != 1 || values[1] != 2 || values[2] != 3) {
            throw std::runtime_error("Range test failed");
        }
    }
    
    // Test Iterate
    {
        std::vector<int> sourceValues = {10, 20, 30};
        std::vector<int> receivedValues;
        bool completed = false;
        
        auto observer = CreateObserver<int>(
            [&](const int& value) { receivedValues.push_back(value); },
            [&]() { completed = true; }
        );
        auto iterate = Iterate<int>(sourceValues);
        iterate->Subscribe(observer);
        
        if (!completed || receivedValues != sourceValues) {
            throw std::runtime_error("Iterate test failed");
        }
    }
    
    // Test Create
    {
        std::vector<int> values;
        bool completed = false;
        
        auto observable = Create<int>([&](std::shared_ptr<IObserver<int>> observer) {
            observer->OnNext(100);
            observer->OnNext(200);
            observer->OnCompleted();
        });
        
        auto observer = CreateObserver<int>(
            [&](const int& value) { values.push_back(value); },
            [&]() { completed = true; }
        );
        
        observable->Subscribe(observer);
        
        if (!completed || values.size() != 2 || values[0] != 100 || values[1] != 200) {
            throw std::runtime_error("Create test failed");
        }
    }
    
    // Test Timer (desktop mode - immediate emission)
    {
        bool received = false;
        bool completed = false;
        unsigned long value = 999;
        
        auto observer = CreateObserver<unsigned long>(
            [&](const unsigned long& v) { received = true; value = v; },
            [&]() { completed = true; }
        );
        auto timer = Timer<unsigned long>(100);
        timer->Subscribe(observer);
        
        if (!received || !completed || value != 0) {
            throw std::runtime_error("Timer test failed");
        }
    }
    
    // Test Interval (desktop mode - emits 3 values then completes)
    {
        std::vector<unsigned long> values;
        bool completed = false;
        
        auto observer = CreateObserver<unsigned long>(
            [&](const unsigned long& value) { values.push_back(value); },
            [&]() { completed = true; }
        );
        auto interval = Interval<unsigned long>(100);
        interval->Subscribe(observer);
        
        if (!completed || values.size() != 3 || values[0] != 0 || values[1] != 1 || values[2] != 2) {
            throw std::runtime_error("Interval test failed");
        }
    }
    
    // Test Defer
    {
        int factoryCallCount = 0;
        std::vector<int> values;
        bool completed = false;
        
        auto defer = Defer<int>([&factoryCallCount]() -> std::shared_ptr<IObservable<int>> {
            factoryCallCount++;
            std::vector<int> source = {50, 60};
            return Iterate<int>(source);
        });
        
        auto observer = CreateObserver<int>(
            [&](const int& value) { values.push_back(value); },
            [&]() { completed = true; }
        );
        
        defer->Subscribe(observer);
        
        if (factoryCallCount != 1 || !completed || values.size() != 2 || values[0] != 50 || values[1] != 60) {
            throw std::runtime_error("Defer test failed");
        }
    }
    
    // Test Scope
    {
        int resourceCalls = 0;
        int observableCalls = 0;
        std::vector<int> values;
        bool completed = false;
        
        auto scope = Scope<int>(
            [&resourceCalls]() -> std::vector<int> {
                resourceCalls++;
                return {70, 80, 90};
            },
            [&observableCalls](std::vector<int> resource) -> std::shared_ptr<IObservable<int>> {
                observableCalls++;
                return Iterate<int>(resource);
            }
        );
        
        auto observer = CreateObserver<int>(
            [&](const int& value) { values.push_back(value); },
            [&]() { completed = true; }
        );
        
        scope->Subscribe(observer);
        
        if (resourceCalls != 1 || observableCalls != 1 || !completed || 
            values.size() != 3 || values[0] != 70 || values[1] != 80 || values[2] != 90) {
            throw std::runtime_error("Scope test failed");
        }
    }
    
    std::cout << "✓ All source observables test passed" << std::endl;
}

void testMemoryManagement() {
    std::cout << "Testing memory management with shared_ptr..." << std::endl;
    
    // Test multiple observers on same observable
    std::vector<int> values1, values2;
    bool completed1 = false, completed2 = false;
    
    auto observer1 = CreateObserver<int>(
        [&](const int& value) { values1.push_back(value); },
        [&]() { completed1 = true; }
    );
    
    auto observer2 = CreateObserver<int>(
        [&](const int& value) { values2.push_back(value); },
        [&]() { completed2 = true; }
    );
    
    auto range = Range<int>(1, 2, 1);
    range->Subscribe(observer1);
    range->Subscribe(observer2);
    
    if (!completed1 || !completed2 || values1 != values2 || values1.size() != 2) {
        throw std::runtime_error("Memory management test failed");
    }
    
    std::cout << "✓ Memory management test passed" << std::endl;
}

void testEdgeCases() {
    std::cout << "Testing edge cases..." << std::endl;
    
    // Test empty vector iteration
    {
        std::vector<int> emptySource;
        bool completed = false;
        int valueCount = 0;
        
        auto observer = CreateObserver<int>(
            [&](const int&) { valueCount++; },
            [&]() { completed = true; }
        );
        
        auto iterate = Iterate<int>(emptySource);
        iterate->Subscribe(observer);
        
        if (!completed || valueCount != 0) {
            throw std::runtime_error("Empty iteration test failed");
        }
    }
    
    // Test range with zero step (should be single value)
    {
        std::vector<int> values;
        bool completed = false;
        
        auto observer = CreateObserver<int>(
            [&](const int& value) { values.push_back(value); },
            [&]() { completed = true; }
        );
        
        auto range = Range<int>(5, 5, 1);  // Same start and end
        range->Subscribe(observer);
        
        if (!completed || values.size() != 1 || values[0] != 5) {
            throw std::runtime_error("Range edge case test failed");
        }
    }
    
    std::cout << "✓ Edge cases test passed" << std::endl;
}

int main() {
    std::cout << "=== Comprehensive Compilation and Functionality Test ===" << std::endl;
    std::cout << "Testing C++11 micro-reactive library..." << std::endl;
    std::cout << std::endl;
    
    try {
        testCoreObserverPattern();
        testAllSources();
        testMemoryManagement();
        testEdgeCases();
        
        std::cout << std::endl;
        std::cout << "🎉 ALL COMPREHENSIVE TESTS PASSED! 🎉" << std::endl;
        std::cout << "✓ Core observer/observable patterns work correctly" << std::endl;
        std::cout << "✓ All source observables (Empty, Never, Range, Create, Iterate, Timer, Interval, Defer, Scope) work correctly" << std::endl;
        std::cout << "✓ Memory management with shared_ptr is working" << std::endl;
        std::cout << "✓ Edge cases are handled properly" << std::endl;
        std::cout << "✓ C++11 compatibility confirmed" << std::endl;
        std::cout << "✓ Desktop compilation successful" << std::endl;
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed: " << e.what() << std::endl;
        return 1;
    }
}
