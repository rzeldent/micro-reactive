#include <iostream>
#include <memory>
#include "micro-reactive.h"

class SimpleObserver : public rx::IObserver<int> {
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
    std::cout << "Testing operators step by step..." << std::endl;
    
    // Test 1: Basic Range
    auto source = rx::Range(1, 3, 1);
    auto observer = std::make_shared<SimpleObserver>();
    
    std::cout << "\nTest 1: Range source" << std::endl;
    source->Subscribe(observer);
    
    // Test 2: Map operator 
    std::cout << "\nTest 2: Map operator" << std::endl;
    std::shared_ptr<rx::IObservable<int>> observableSource = source;
    auto mapped = rx::Map<int, int>(observableSource, [](const int& x) { return x * 2; });
    mapped->Subscribe(observer);
    
    std::cout << "\nOperator test completed!" << std::endl;
    return 0;
}
