/**
 * Arduino Example for Micro-Reactive Library (Simplified Structure)
 * 
 * This example demonstrates basic usage of the micro-reactive library
 * on ESP32/Arduino platforms using the new simplified structure.
 * Now includes additional operators: Distinct, Scan, Reduce, First, Last, etc.
 */

#include <Arduino.h>
#include "micro-reactive.h"

class ArduinoObserver : public rx::IObserver<int> {
public:
    ArduinoObserver(const String& name) : name_(name) {}
    
    void OnNext(const int& value) override {
        Serial.print(name_);
        Serial.print(": ");
        Serial.println(value);
    }
    
    void OnCompleted() override {
        Serial.print(name_);
        Serial.println(" completed");
    }
    
    void OnError(const std::exception& e) override {
        Serial.print(name_);
        Serial.print(" error: ");
        Serial.println(e.what());
    }
    
private:
    String name_;
};

void setup() {
    Serial.begin(115200);
    delay(1000);
    
    Serial.println("Micro-Reactive Arduino Example with Additional Operators");
    Serial.println("=======================================================");
    
    // Example 1: Simple Range Observable
    Serial.println("\n1. Range Observable (1 to 5):");
    auto range_obs = rx::Range(1, 5, 1);
    auto range_observer = std::make_shared<ArduinoObserver>("Range");
    range_obs->Subscribe(range_observer);
    
    // Example 2: Map Operator
    Serial.println("\n2. Range with Map (multiply by 2):");
    auto range_obs2 = rx::Range(1, 3, 1);
    std::shared_ptr<rx::IObservable<int>> obs = range_obs2;
    std::function<int(const int&)> mapFunc = [](const int& x) { return x * 2; };
    auto mapped_obs = rx::Map<int, int>(obs, mapFunc);
    auto map_observer = std::make_shared<ArduinoObserver>("Map");
    mapped_obs->Subscribe(map_observer);
    
    // Example 3: Filter Operator
    Serial.println("\n3. Range with Filter (even numbers only):");
    auto range_obs3 = rx::Range(1, 10, 1);
    std::shared_ptr<rx::IObservable<int>> obs2 = range_obs3;
    std::function<bool(const int&)> filterFunc = [](const int& x) { return x % 2 == 0; };
    auto filtered_obs = rx::Filter(obs2, filterFunc);
    auto filter_observer = std::make_shared<ArduinoObserver>("Filter");
    filtered_obs->Subscribe(filter_observer);
    
    // Example 4: NEW - Distinct Operator (removes duplicates)
    Serial.println("\n4. NEW - Distinct Operator (1,2,2,3,3,4):");
    std::vector<int> values = {1, 2, 2, 3, 3, 4};
    auto iterate_obs = rx::Iterate<int>(values);
    std::shared_ptr<rx::IObservable<int>> obs3 = iterate_obs;
    auto distinct_obs = rx::Distinct(obs3);
    auto distinct_observer = std::make_shared<ArduinoObserver>("Distinct");
    distinct_obs->Subscribe(distinct_observer);
    
    // Example 5: NEW - Scan Operator (running sum)
    Serial.println("\n5. NEW - Scan Operator (running sum 1-5):");
    auto range_obs4 = rx::Range(1, 5, 1);
    std::shared_ptr<rx::IObservable<int>> obs4 = range_obs4;
    auto scan_obs = rx::Scan<int, int>(obs4, 0, [](const int& acc, const int& val) { return acc + val; });
    auto scan_observer = std::make_shared<ArduinoObserver>("Scan");
    scan_obs->Subscribe(scan_observer);
    
    // Example 6: NEW - Reduce Operator (final sum)
    Serial.println("\n6. NEW - Reduce Operator (total sum 1-5):");
    auto range_obs5 = rx::Range(1, 5, 1);
    std::shared_ptr<rx::IObservable<int>> obs5 = range_obs5;
    auto reduce_obs = rx::Reduce<int, int>(obs5, 0, [](const int& acc, const int& val) { return acc + val; });
    auto reduce_observer = std::make_shared<ArduinoObserver>("Reduce");
    reduce_obs->Subscribe(reduce_observer);
    
    // Example 7: NEW - First Operator
    Serial.println("\n7. NEW - First Operator (first from 1-10):");
    auto range_obs6 = rx::Range(1, 10, 1);
    std::shared_ptr<rx::IObservable<int>> obs6 = range_obs6;
    auto first_obs = rx::First(obs6);
    auto first_observer = std::make_shared<ArduinoObserver>("First");
    first_obs->Subscribe(first_observer);
    
    // Example 8: NEW - Last Operator
    Serial.println("\n8. NEW - Last Operator (last from 1-5):");
    auto range_obs7 = rx::Range(1, 5, 1);
    std::shared_ptr<rx::IObservable<int>> obs7 = range_obs7;
    auto last_obs = rx::Last(obs7);
    auto last_observer = std::make_shared<ArduinoObserver>("Last");
    last_obs->Subscribe(last_observer);
    
    // Example 9: NEW - Throttle Operator (every 3rd item)
    Serial.println("\n9. NEW - Throttle Operator (every 3rd from 1-10):");
    auto range_obs8 = rx::Range(1, 10, 1);
    std::shared_ptr<rx::IObservable<int>> obs8 = range_obs8;
    auto throttle_obs = rx::Throttle(obs8, 3);
    auto throttle_observer = std::make_shared<ArduinoObserver>("Throttle");
    throttle_obs->Subscribe(throttle_observer);
    
    // Example 10: NEW - LINQ-style aliases (Where = Filter, Select = Map)
    Serial.println("\n10. NEW - LINQ-style aliases (Where/Select):");
    auto range_obs9 = rx::Range(1, 6, 1);
    std::shared_ptr<rx::IObservable<int>> obs9 = range_obs9;
    std::function<bool(const int&)> whereFunc = [](const int& x) { return x > 3; };
    auto where_obs = rx::Where(obs9, whereFunc);
    std::function<int(const int&)> selectFunc = [](const int& x) { return x * 10; };
    auto select_obs = rx::Select<int, int>(where_obs, selectFunc);
    auto linq_observer = std::make_shared<ArduinoObserver>("LINQ");
    select_obs->Subscribe(linq_observer);
    
    // Example 11: Subject
    Serial.println("\n11. Subject Example:");
    auto subject = rx::Subject<int>();
    auto subject_observer = std::make_shared<ArduinoObserver>("Subject");
    subject.Subscribe(subject_observer);
    
    // Emit values
    subject.OnNext(100);
    subject.OnNext(200);
    subject.OnNext(300);
    
    // Example 12: BehaviorSubject with initial value
    Serial.println("\n12. BehaviorSubject Example:");
    auto behavior = rx::BehaviorSubject<int>(42);
    auto behavior_observer = std::make_shared<ArduinoObserver>("Behavior");
    behavior.Subscribe(behavior_observer);
    
    behavior.OnNext(43);
    behavior.OnNext(44);
    
    // Example 13: NEW - Buffer Operator (groups into batches)
    Serial.println("\n13. NEW - Buffer Operator (groups 1-7 into batches of 3):");
    auto range_obs10 = rx::Range(1, 7, 1);
    std::shared_ptr<rx::IObservable<int>> obs10 = range_obs10;
    auto buffer_obs = rx::Buffer(obs10, 3);
    // Note: Buffer returns vector<int>, would need custom observer for Arduino
    Serial.println("Buffer would group: [1,2,3], [4,5,6], [7]");
    
    // Example 14: NEW - TakeWhile Operator (takes while condition is true)
    Serial.println("\n14. NEW - TakeWhile Operator (take while < 6 from 1-10):");
    auto range_obs11 = rx::Range(1, 10, 1);
    std::shared_ptr<rx::IObservable<int>> obs11 = range_obs11;
    std::function<bool(const int&)> takewhileFunc = [](const int& x) { return x < 6; };
    auto takewhile_obs = rx::TakeWhile(obs11, takewhileFunc);
    auto takewhile_observer = std::make_shared<ArduinoObserver>("TakeWhile");
    takewhile_obs->Subscribe(takewhile_observer);
    
    // Example 15: NEW - Count Operator (counts items)
    Serial.println("\n15. NEW - Count Operator (count items 1-8):");
    auto range_obs12 = rx::Range(1, 8, 1);
    std::shared_ptr<rx::IObservable<int>> obs12 = range_obs12;
    auto count_obs = rx::Count(obs12);
    // Note: Count returns size_t, would need custom observer for Arduino
    Serial.println("Count would return: 8");
    
    // Example 16: NEW - Sum Operator (sums values)
    Serial.println("\n16. NEW - Sum Operator (sum 1-5):");
    auto range_obs13 = rx::Range(1, 5, 1);
    std::shared_ptr<rx::IObservable<int>> obs13 = range_obs13;
    auto sum_obs = rx::Sum(obs13);
    auto sum_observer = std::make_shared<ArduinoObserver>("Sum");
    sum_obs->Subscribe(sum_observer);
    
    Serial.println("\nExample completed! New operators added:");
    Serial.println("- Distinct: Remove duplicates");
    Serial.println("- Scan: Running accumulation");
    Serial.println("- Reduce: Final accumulation");
    Serial.println("- First: Only first item");
    Serial.println("- Last: Only last item");
    Serial.println("- Throttle: Every nth item");
    Serial.println("- Where/Select: LINQ-style aliases");
    Serial.println("- Buffer: Group items into batches");
    Serial.println("- TakeWhile: Take while condition is true");
    Serial.println("- SkipWhile: Skip while condition is true");
    Serial.println("- StartWith: Prepend values to sequence");
    Serial.println("- DefaultIfEmpty: Emit default if empty");
    Serial.println("- Count: Count number of items");
    Serial.println("- Sum: Sum of numeric items");
    Serial.println("- Min/Max: Find minimum/maximum values");
}

void loop() {
    // Nothing to do in loop for this example
    delay(1000);
}
