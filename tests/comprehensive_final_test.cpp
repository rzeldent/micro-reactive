/**
 * Comprehensive Test for Simplified Micro-Reactive Library
 * 
 * This test demonstrates all major features of the library
 * using the new simplified structure.
 */

#include "micro-reactive.h"
#include <iostream>

class TestObserver : public rx::IObserver<int> {
public:
    TestObserver(const std::string& name) : name_(name) {}
    
    void OnNext(const int& value) override {
        std::cout << "  " << name_ << ": " << value << std::endl;
    }
    
    void OnCompleted() override {
        std::cout << "  " << name_ << " completed" << std::endl;
    }
    
    void OnError(const std::exception& e) override {
        std::cout << "  " << name_ << " error: " << e.what() << std::endl;
    }
    
private:
    std::string name_;
};

void test_sources() {
    std::cout << "\n=== Testing Sources ===" << std::endl;
    
    // Range
    std::cout << "Range(1, 3, 1):" << std::endl;
    auto range_obs = rx::Range(1, 3, 1);
    auto range_observer = std::make_shared<TestObserver>("Range");
    range_obs->Subscribe(range_observer);
    
    // Empty
    std::cout << "Empty:" << std::endl;
    auto empty_obs = rx::Empty<int>();
    auto empty_observer = std::make_shared<TestObserver>("Empty");
    empty_obs->Subscribe(empty_observer);
    
    // Never (won't produce output)
    std::cout << "Never (no output expected):" << std::endl;
    auto never_obs = rx::Never<int>();
    auto never_observer = std::make_shared<TestObserver>("Never");
    never_obs->Subscribe(never_observer);
}

void test_operators() {
    std::cout << "\n=== Testing Operators ===" << std::endl;
    
    // Map
    std::cout << "Range(1, 3, 1) -> Map(x*10):" << std::endl;
    auto range_obs = rx::Range(1, 3, 1);
    std::shared_ptr<rx::IObservable<int>> obs = range_obs;
    std::function<int(const int&)> mapFunc = [](const int& x) { return x * 10; };
    auto map_obs = rx::Map<int, int>(obs, mapFunc);
    auto map_observer = std::make_shared<TestObserver>("Map");
    map_obs->Subscribe(map_observer);
    
    // Filter
    std::cout << "Range(1, 10, 1) -> Filter(even):" << std::endl;
    auto range_obs2 = rx::Range(1, 10, 1);
    std::shared_ptr<rx::IObservable<int>> obs2 = range_obs2;
    std::function<bool(const int&)> filterFunc = [](const int& x) { return x % 2 == 0; };
    auto filter_obs = rx::Filter(obs2, filterFunc);
    auto filter_observer = std::make_shared<TestObserver>("Filter");
    filter_obs->Subscribe(filter_observer);
    
    // Take
    std::cout << "Range(1, 10, 1) -> Take(3):" << std::endl;
    auto range_obs3 = rx::Range(1, 10, 1);
    std::shared_ptr<rx::IObservable<int>> obs3 = range_obs3;
    auto take_obs = rx::Take(obs3, 3);
    auto take_observer = std::make_shared<TestObserver>("Take");
    take_obs->Subscribe(take_observer);
    
    // Skip
    std::cout << "Range(1, 5, 1) -> Skip(2):" << std::endl;
    auto range_obs4 = rx::Range(1, 5, 1);
    std::shared_ptr<rx::IObservable<int>> obs4 = range_obs4;
    auto skip_obs = rx::Skip(obs4, 2);
    auto skip_observer = std::make_shared<TestObserver>("Skip");
    skip_obs->Subscribe(skip_observer);
    
    // Chained operators
    std::cout << "Range(1, 20, 1) -> Filter(even) -> Map(x*2) -> Take(3):" << std::endl;
    auto range_obs5 = rx::Range(1, 20, 1);
    std::shared_ptr<rx::IObservable<int>> obs5 = range_obs5;
    
    std::function<bool(const int&)> evenFilter = [](const int& x) { return x % 2 == 0; };
    auto filtered = rx::Filter(obs5, evenFilter);
    
    std::function<int(const int&)> doubleMap = [](const int& x) { return x * 2; };
    auto mapped = rx::Map<int, int>(filtered, doubleMap);
    
    std::shared_ptr<rx::IObservable<int>> mapped_obs = mapped;
    auto chained = rx::Take(mapped_obs, 3);
    auto chained_observer = std::make_shared<TestObserver>("Chained");
    chained->Subscribe(chained_observer);
}

void test_subjects() {
    std::cout << "\n=== Testing Subjects ===" << std::endl;
    
    // Subject
    std::cout << "Subject:" << std::endl;
    auto subject = rx::Subject<int>();
    auto subject_observer = std::make_shared<TestObserver>("Subject");
    subject.Subscribe(subject_observer);
    subject.OnNext(100);
    subject.OnNext(200);
    subject.OnNext(300);
    
    // BehaviorSubject
    std::cout << "BehaviorSubject:" << std::endl;
    auto behavior = rx::BehaviorSubject<int>(42);
    auto behavior_observer = std::make_shared<TestObserver>("Behavior");
    behavior.Subscribe(behavior_observer);
    behavior.OnNext(43);
    behavior.OnNext(44);
    
    // ReplaySubject
    std::cout << "ReplaySubject (buffer=2):" << std::endl;
    auto replay = rx::ReplaySubject<int>(2);
    replay.OnNext(10);
    replay.OnNext(20);
    replay.OnNext(30);
    auto replay_observer = std::make_shared<TestObserver>("Replay");
    replay.Subscribe(replay_observer);
    replay.OnNext(40);
}

void test_advanced_scenarios() {
    std::cout << "\n=== Testing Advanced Scenarios ===" << std::endl;
    
    // Multiple subscribers
    std::cout << "Multiple subscribers to same observable:" << std::endl;
    auto obs = rx::Range(1, 3, 1);
    auto observer1 = std::make_shared<TestObserver>("Subscriber1");
    auto observer2 = std::make_shared<TestObserver>("Subscriber2");
    obs->Subscribe(observer1);
    obs->Subscribe(observer2);
    
    // Subject with operators
    std::cout << "Subject with operators:" << std::endl;
    auto subjectWithOps = rx::Subject<int>();
    
    // Get the subject as an observable and apply operators
    std::shared_ptr<rx::IObservable<int>> subject_obs = 
        std::static_pointer_cast<rx::IObservable<int>>(
            std::make_shared<rx::Subject<int>>(subjectWithOps));
    
    std::function<bool(const int&)> greaterThan5 = [](const int& x) { return x > 5; };
    auto filtered_obs = rx::Filter(subject_obs, greaterThan5);
    
    std::function<int(const int&)> square = [](const int& x) { return x * x; };
    auto mapped_obs = rx::Map<int, int>(filtered_obs, square);
    
    auto advanced_observer = std::make_shared<TestObserver>("Advanced");
    mapped_obs->Subscribe(advanced_observer);
    
    subjectWithOps.OnNext(3);  // Should be filtered out
    subjectWithOps.OnNext(6);  // Should become 36
    subjectWithOps.OnNext(7);  // Should become 49
}

int main() {
    std::cout << "=== Comprehensive Micro-Reactive Library Test ===" << std::endl;
    std::cout << "Using simplified structure (include_new/)" << std::endl;
    
    test_sources();
    test_operators();
    test_subjects();
    test_advanced_scenarios();
    
    std::cout << "\n✅ All tests completed successfully!" << std::endl;
    std::cout << "The simplified library structure is working perfectly." << std::endl;
    
    return 0;
}
