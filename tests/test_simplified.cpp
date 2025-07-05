#include <iostream>
#include <memory>
#include "../include/micro-reactive.h"

class TestObserver : public rx::IObserver<int> {
public:
    void OnNext(const int& value) override {
        std::cout << "Value: " << value << std::endl;
    }
    
    void OnCompleted() override {
        std::cout << "Completed" << std::endl;
    }
    
    void OnError(const std::exception& e) override {
        std::cout << "Error: " << e.what() << std::endl;
    }
};

int main() {
    std::cout << "=== Simplified Micro-Reactive Library Test ===" << std::endl;
    
    auto observer = std::make_shared<TestObserver>();
    
    // Test 1: Basic Range source
    std::cout << "\n1. Range(1, 5, 1):" << std::endl;
    auto range = rx::Range(1, 5, 1);
    range->Subscribe(observer);
    
    // Test 2: Empty source
    std::cout << "\n2. Empty:" << std::endl;
    auto empty = rx::Empty<int>();
    empty->Subscribe(observer);
    
    // Test 3: Map operator
    std::cout << "\n3. Range -> Map(x*2):" << std::endl;
    auto source = rx::Range(1, 3, 1);
    std::shared_ptr<rx::IObservable<int>> obs = source;
    std::function<int(const int&)> mapFunc = [](const int& x) { return x * 2; };
    auto mapped = rx::Map<int, int>(obs, mapFunc);
    mapped->Subscribe(observer);
    
    // Test 4: Filter operator
    std::cout << "\n4. Range -> Filter(even):" << std::endl;
    auto source2 = rx::Range(1, 6, 1);
    std::shared_ptr<rx::IObservable<int>> obs2 = source2;
    std::function<bool(const int&)> filterFunc = [](const int& x) { return x % 2 == 0; };
    auto filtered = rx::Filter(obs2, filterFunc);
    filtered->Subscribe(observer);
    
    // Test 5: Subject
    std::cout << "\n5. Subject:" << std::endl;
    auto subject = rx::CreateSubject<int>();
    subject->Subscribe(observer);
    subject->OnNext(100);
    subject->OnNext(200);
    subject->OnCompleted();
    
    // Test 6: BehaviorSubject
    std::cout << "\n6. BehaviorSubject:" << std::endl;
    auto behaviorSubject = rx::CreateBehaviorSubject<int>(42);
    behaviorSubject->Subscribe(observer);
    behaviorSubject->OnNext(300);
    behaviorSubject->OnCompleted();
    
    std::cout << "\n✅ Simplified library test completed successfully!" << std::endl;
    return 0;
}
