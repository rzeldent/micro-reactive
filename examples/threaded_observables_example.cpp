#include <micro-reactive.h>
#include <iostream>
#include <chrono>

using namespace rx;

class SimpleObserver : public IObserver<int> {
private:
    std::string name_;

public:
    SimpleObserver(const std::string& name) : name_(name) {}

    void OnNext(const int& value) override {
        std::cout << "[" << name_ << "] OnNext: " << value << std::endl;
    }

    void OnCompleted() override {
        std::cout << "[" << name_ << "] OnCompleted" << std::endl;
    }

    void OnError(const std::exception& e) override {
        std::cout << "[" << name_ << "] OnError: " << e.what() << std::endl;
    }
};

int main() {
    std::cout << "=== Threaded Timer Observable Demo ===" << std::endl;
    
    // Create a timer observable that emits after 2 seconds
    auto timer = Timer<int>(std::chrono::milliseconds(2000));
    auto observer1 = std::make_shared<SimpleObserver>("Timer");
    
    std::cout << "Starting timer (2 seconds)..." << std::endl;
    timer->Subscribe(observer1);
    
    // Main thread continues - timer runs in background
    std::cout << "Main thread continues immediately..." << std::endl;
    
    // Wait a bit to see timer result
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    
    std::cout << "\n=== Threaded Interval Observable Demo ===" << std::endl;
    
    // Create an interval observable that emits 3 values every 500ms
    auto interval = Interval<int>(std::chrono::milliseconds(500), 3);
    auto observer2 = std::make_shared<SimpleObserver>("Interval");
    
    std::cout << "Starting interval (500ms, 3 emissions)..." << std::endl;
    interval->Subscribe(observer2);
    
    // Main thread continues - interval runs in background
    std::cout << "Main thread continues immediately..." << std::endl;
    
    // Wait for interval to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    
    std::cout << "\n=== Demo Complete ===" << std::endl;
    
    return 0;
}
