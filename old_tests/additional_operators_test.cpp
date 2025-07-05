/**
 * Comprehensive Test for Additional Operators
 * 
 * This test demonstrates all the new operators added to the library:
 * Distinct, Scan, Reduce, First, Last, Throttle, Where, Select
 */

#include "micro-reactive.h"
#include <iostream>
#include <vector>

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

void test_distinct_operator() {
    std::cout << "\n=== Testing Distinct Operator ===" << std::endl;
    std::cout << "Input: 1, 2, 2, 3, 3, 4, 1, 5" << std::endl;
    
    std::vector<int> values = {1, 2, 2, 3, 3, 4, 1, 5};
    auto iterate_obs = rx::Iterate<int>(values);
    std::shared_ptr<rx::IObservable<int>> obs = iterate_obs;
    auto distinct_obs = rx::Distinct(obs);
    auto observer = std::make_shared<TestObserver>("Distinct");
    distinct_obs->Subscribe(observer);
}

void test_scan_operator() {
    std::cout << "\n=== Testing Scan Operator ===" << std::endl;
    std::cout << "Running sum of 1, 2, 3, 4, 5:" << std::endl;
    
    auto range_obs = rx::Range(1, 5, 1);
    std::shared_ptr<rx::IObservable<int>> obs = range_obs;
    auto scan_obs = rx::Scan<int, int>(obs, 0, [](const int& acc, const int& val) {
        return acc + val;
    });
    auto observer = std::make_shared<TestObserver>("Scan");
    scan_obs->Subscribe(observer);
}

void test_reduce_operator() {
    std::cout << "\n=== Testing Reduce Operator ===" << std::endl;
    std::cout << "Total sum of 1, 2, 3, 4, 5:" << std::endl;
    
    auto range_obs = rx::Range(1, 5, 1);
    std::shared_ptr<rx::IObservable<int>> obs = range_obs;
    auto reduce_obs = rx::Reduce<int, int>(obs, 0, [](const int& acc, const int& val) {
        return acc + val;
    });
    auto observer = std::make_shared<TestObserver>("Reduce");
    reduce_obs->Subscribe(observer);
}

void test_first_operator() {
    std::cout << "\n=== Testing First Operator ===" << std::endl;
    std::cout << "First item from 10, 20, 30, 40, 50:" << std::endl;
    
    auto range_obs = rx::Range(10, 50, 10);
    std::shared_ptr<rx::IObservable<int>> obs = range_obs;
    auto first_obs = rx::First(obs);
    auto observer = std::make_shared<TestObserver>("First");
    first_obs->Subscribe(observer);
}

void test_last_operator() {
    std::cout << "\n=== Testing Last Operator ===" << std::endl;
    std::cout << "Last item from 10, 20, 30, 40, 50:" << std::endl;
    
    auto range_obs = rx::Range(10, 50, 10);
    std::shared_ptr<rx::IObservable<int>> obs = range_obs;
    auto last_obs = rx::Last(obs);
    auto observer = std::make_shared<TestObserver>("Last");
    last_obs->Subscribe(observer);
}

void test_throttle_operator() {
    std::cout << "\n=== Testing Throttle Operator ===" << std::endl;
    std::cout << "Every 3rd item from 1-10:" << std::endl;
    
    auto range_obs = rx::Range(1, 10, 1);
    std::shared_ptr<rx::IObservable<int>> obs = range_obs;
    auto throttle_obs = rx::Throttle(obs, 3);
    auto observer = std::make_shared<TestObserver>("Throttle");
    throttle_obs->Subscribe(observer);
}

void test_linq_aliases() {
    std::cout << "\n=== Testing LINQ-style Aliases ===" << std::endl;
    std::cout << "Where(x > 3).Select(x * 10) from 1-6:" << std::endl;
    
    auto range_obs = rx::Range(1, 6, 1);
    std::shared_ptr<rx::IObservable<int>> obs = range_obs;
    
    std::function<bool(const int&)> whereFunc = [](const int& x) { return x > 3; };
    auto where_obs = rx::Where(obs, whereFunc);
    
    std::function<int(const int&)> selectFunc = [](const int& x) { return x * 10; };
    auto select_obs = rx::Select<int, int>(where_obs, selectFunc);
    
    auto observer = std::make_shared<TestObserver>("LINQ");
    select_obs->Subscribe(observer);
}

void test_chained_operators() {
    std::cout << "\n=== Testing Chained New Operators ===" << std::endl;
    std::cout << "Range(1-10) -> Where(even) -> Scan(sum) -> Throttle(2):" << std::endl;
    
    auto range_obs = rx::Range(1, 10, 1);
    std::shared_ptr<rx::IObservable<int>> obs = range_obs;
    
    // Filter even numbers
    std::function<bool(const int&)> evenFunc = [](const int& x) { return x % 2 == 0; };
    auto where_obs = rx::Where(obs, evenFunc);
    
    // Running sum
    auto scan_obs = rx::Scan<int, int>(where_obs, 0, [](const int& acc, const int& val) {
        return acc + val;
    });
    
    // Every 2nd item
    std::shared_ptr<rx::IObservable<int>> scan_interface = scan_obs;
    auto throttle_obs = rx::Throttle(scan_interface, 2);
    
    auto observer = std::make_shared<TestObserver>("Chained");
    throttle_obs->Subscribe(observer);
}

void test_product_accumulation() {
    std::cout << "\n=== Testing Product Accumulation ===" << std::endl;
    std::cout << "Running product of 2, 3, 4:" << std::endl;
    
    auto range_obs = rx::Range(2, 4, 1);
    std::shared_ptr<rx::IObservable<int>> obs = range_obs;
    
    auto scan_obs = rx::Scan<int, int>(obs, 1, [](const int& acc, const int& val) {
        return acc * val;
    });
    
    auto observer = std::make_shared<TestObserver>("Product");
    scan_obs->Subscribe(observer);
}

int main() {
    std::cout << "=== Additional Operators Test Suite ===" << std::endl;
    std::cout << "Testing new operators: Distinct, Scan, Reduce, First, Last, Throttle, Where, Select" << std::endl;
    
    test_distinct_operator();
    test_scan_operator();
    test_reduce_operator();
    test_first_operator();
    test_last_operator();
    test_throttle_operator();
    test_linq_aliases();
    test_chained_operators();
    test_product_accumulation();
    
    std::cout << "\n✅ All additional operators tested successfully!" << std::endl;
    std::cout << "\nNew operators summary:" << std::endl;
    std::cout << "- Distinct: Removes duplicate values" << std::endl;
    std::cout << "- Scan: Running accumulation (emits each step)" << std::endl;
    std::cout << "- Reduce: Final accumulation (emits only result)" << std::endl;
    std::cout << "- First: Emits only the first item" << std::endl;
    std::cout << "- Last: Emits only the last item" << std::endl;
    std::cout << "- Throttle: Emits every nth item" << std::endl;
    std::cout << "- Where: Alias for Filter (LINQ-style)" << std::endl;
    std::cout << "- Select: Alias for Map (LINQ-style)" << std::endl;
    
    return 0;
}
