#include "../include/micro-reactive.h"
#include <iostream>
#include <vector>
#include <string>

// Simple observer for testing
template<typename T>
class TestObserver : public rx::IObserver<T> {
private:
    std::string _name;
    std::vector<T> _values;
    bool _completed;
    bool _errored;

public:
    TestObserver(const std::string& name) : _name(name), _completed(false), _errored(false) {}

    void OnNext(const T& value) override {
        _values.push_back(value);
        std::cout << _name << " received: " << value << std::endl;
    }

    void OnCompleted() override {
        _completed = true;
        std::cout << _name << " completed!" << std::endl;
    }

    void OnError(const std::exception& e) override {
        _errored = true;
        std::cout << _name << " error: " << e.what() << std::endl;
    }

    const std::vector<T>& GetValues() const { return _values; }
    bool IsCompleted() const { return _completed; }
    bool IsErrored() const { return _errored; }
    size_t Count() const { return _values.size(); }
};

// Specialization for vector output
template<>
class TestObserver<std::vector<int>> : public rx::IObserver<std::vector<int>> {
private:
    std::string _name;
    std::vector<std::vector<int>> _values;
    bool _completed;
    bool _errored;

public:
    TestObserver(const std::string& name) : _name(name), _completed(false), _errored(false) {}

    void OnNext(const std::vector<int>& value) override {
        _values.push_back(value);
        std::cout << _name << " received buffer: [";
        for (size_t i = 0; i < value.size(); ++i) {
            std::cout << value[i];
            if (i < value.size() - 1) std::cout << ", ";
        }
        std::cout << "]" << std::endl;
    }

    void OnCompleted() override {
        _completed = true;
        std::cout << _name << " completed!" << std::endl;
    }

    void OnError(const std::exception& e) override {
        _errored = true;
        std::cout << _name << " error: " << e.what() << std::endl;
    }

    const std::vector<std::vector<int>>& GetValues() const { return _values; }
    bool IsCompleted() const { return _completed; }
    bool IsErrored() const { return _errored; }
    size_t Count() const { return _values.size(); }
};

void testNewOperators() {
    std::cout << "\n=== Testing New Additional Operators ===" << std::endl;

    // Test Buffer operator
    std::cout << "\n--- Testing Buffer operator ---" << std::endl;
    {
        auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 7));
        auto buffered = rx::Buffer(source, 3);
        auto observer = std::make_shared<TestObserver<std::vector<int>>>("Buffer");
        buffered->Subscribe(observer);
        
        std::cout << "Expected: 2 buffers: [1,2,3] and [4,5,6,7]" << std::endl;
        std::cout << "Actual buffers received: " << observer->Count() << std::endl;
    }

    // Test TakeWhile operator
    std::cout << "\n--- Testing TakeWhile operator ---" << std::endl;
    {
        auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 10));
        auto takeWhile = rx::TakeWhile<int>(source, [](const int& x) { return x < 5; });
        auto observer = std::make_shared<TestObserver<int>>("TakeWhile");
        takeWhile->Subscribe(observer);
        
        std::cout << "Expected: 1, 2, 3, 4 (takes while < 5)" << std::endl;
        std::cout << "Actual count: " << observer->Count() << std::endl;
    }

    // Test SkipWhile operator
    std::cout << "\n--- Testing SkipWhile operator ---" << std::endl;
    {
        auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 8));
        auto skipWhile = rx::SkipWhile<int>(source, [](const int& x) { return x < 5; });
        auto observer = std::make_shared<TestObserver<int>>("SkipWhile");
        skipWhile->Subscribe(observer);
        
        std::cout << "Expected: 5, 6, 7, 8 (skips while < 5)" << std::endl;
        std::cout << "Actual count: " << observer->Count() << std::endl;
    }

    // Test StartWith operator
    std::cout << "\n--- Testing StartWith operator ---" << std::endl;
    {
        auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(5, 3));
        auto startWith = rx::StartWith<int>(source, {1, 2, 3});
        auto observer = std::make_shared<TestObserver<int>>("StartWith");
        startWith->Subscribe(observer);
        
        std::cout << "Expected: 1, 2, 3, 5, 6, 7 (start with 1,2,3 then range 5-7)" << std::endl;
        std::cout << "Actual count: " << observer->Count() << std::endl;
    }

    // Test DefaultIfEmpty operator with empty source
    std::cout << "\n--- Testing DefaultIfEmpty operator (empty source) ---" << std::endl;
    {
        auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::FromVector<int>({})); // Empty vector
        auto defaultIfEmpty = rx::DefaultIfEmpty<int>(source, 42);
        auto observer = std::make_shared<TestObserver<int>>("DefaultIfEmpty");
        defaultIfEmpty->Subscribe(observer);
        
        std::cout << "Expected: 42 (default value for empty sequence)" << std::endl;
        std::cout << "Actual count: " << observer->Count() << std::endl;
    }

    // Test DefaultIfEmpty operator with non-empty source
    std::cout << "\n--- Testing DefaultIfEmpty operator (non-empty source) ---" << std::endl;
    {
        auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(10, 2));
        auto defaultIfEmpty = rx::DefaultIfEmpty<int>(source, 42);
        auto observer = std::make_shared<TestObserver<int>>("DefaultIfEmpty");
        defaultIfEmpty->Subscribe(observer);
        
        std::cout << "Expected: 10, 11 (original values, no default)" << std::endl;
        std::cout << "Actual count: " << observer->Count() << std::endl;
    }

    // Test Count operator
    std::cout << "\n--- Testing Count operator ---" << std::endl;
    {
        auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 5));
        auto count = rx::Count<int>(source);
        auto observer = std::make_shared<TestObserver<size_t>>("Count");
        count->Subscribe(observer);
        
        std::cout << "Expected: 5 (count of items in range)" << std::endl;
        std::cout << "Actual count: " << observer->Count() << std::endl;
    }

    // Test Sum operator
    std::cout << "\n--- Testing Sum operator ---" << std::endl;
    {
        auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 5));
        auto sum = rx::Sum<int>(source);
        auto observer = std::make_shared<TestObserver<int>>("Sum");
        sum->Subscribe(observer);
        
        std::cout << "Expected: 15 (1+2+3+4+5)" << std::endl;
        std::cout << "Actual count: " << observer->Count() << std::endl;
    }

    // Test Min operator
    std::cout << "\n--- Testing Min operator ---" << std::endl;
    {
        auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::FromVector<int>({5, 2, 8, 1, 9}));
        auto min = rx::Min<int>(source);
        auto observer = std::make_shared<TestObserver<int>>("Min");
        min->Subscribe(observer);
        
        std::cout << "Expected: 1 (minimum value)" << std::endl;
        std::cout << "Actual count: " << observer->Count() << std::endl;
    }

    // Test Max operator
    std::cout << "\n--- Testing Max operator ---" << std::endl;
    {
        auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::FromVector<int>({5, 2, 8, 1, 9}));
        auto max = rx::Max<int>(source);
        auto observer = std::make_shared<TestObserver<int>>("Max");
        max->Subscribe(observer);
        
        std::cout << "Expected: 9 (maximum value)" << std::endl;
        std::cout << "Actual count: " << observer->Count() << std::endl;
    }

    // Test operator chaining with new operators
    std::cout << "\n--- Testing operator chaining with new operators ---" << std::endl;
    {
        auto source = std::static_pointer_cast<rx::IObservable<int>>(rx::Range(1, 10));
        auto chained = rx::TakeWhile<int>(
            rx::StartWith<int>(source, 0),
            [](const int& x) { return x < 8; }
        );
        auto observer = std::make_shared<TestObserver<int>>("Chained");
        chained->Subscribe(observer);
        
        std::cout << "Expected: 0, 1, 2, 3, 4, 5, 6, 7 (StartWith 0, then TakeWhile < 8)" << std::endl;
        std::cout << "Actual count: " << observer->Count() << std::endl;
    }
}

int main() {
    std::cout << "Testing Enhanced Micro-Reactive Library with Additional Operators" << std::endl;
    std::cout << "=================================================================" << std::endl;

    try {
        testNewOperators();
        std::cout << "\n=== All tests completed successfully! ===" << std::endl;
    } catch (const std::exception& e) {
        std::cout << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
