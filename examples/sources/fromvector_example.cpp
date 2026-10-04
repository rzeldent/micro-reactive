#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * FromVector Source Example
 * 
 * Demonstrates the FromVector source which emits all elements
 * from a vector in sequence.
 */

void traditional_fromvector_example() {
    Serial.println("=== Traditional FromVector Example ===");
    
    // Create a vector of strings
    std::vector<std::string> fruits = {"apple", "banana", "orange", "grape"};
    
    // Create observable from vector
    auto vector_observable = FromVector(fruits);
    
    // Create an observer
    auto observer = CreateObserver<std::string>(
        [](const std::string& value) { 
            Serial.print("Fruit: "); 
            Serial.println(value.c_str()); 
        },
        []() { 
            Serial.println("All fruits processed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Error: "); 
            Serial.println(e.what()); 
        }
    );
    
    // Subscribe to the observable
    auto subscription = vector_observable->Subscribe(observer);
    
    Serial.println();
}

void fluent_fromvector_example() {
    Serial.println("=== Fluent FromVector Example ===");
    
    // Create a vector of integers
    std::vector<int> numbers = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    
    // Use fluent interface
    From(FromVector(numbers))
        .Subscribe(
            [](int value) { 
                Serial.print("Number: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("All numbers processed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void fromvector_with_operators_example() {
    Serial.println("=== FromVector with Operators Example ===");
    
    std::vector<int> data = {10, 20, 30, 40, 50};
    
    // Traditional approach
    auto vector_obs = FromVector(data);
    auto filtered_obs = Filter(vector_obs, [](int x) { return x > 25; });
    auto mapped_obs = Map<int, double>(filtered_obs, [](int x) { return x / 10.0; });
    
    mapped_obs->Subscribe(CreateObserver<double>(
        [](double value) { 
            Serial.print("Traditional - Scaled value: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional processing completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional processing error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach
    From(FromVector(data))
        .Filter([](int x) { return x > 25; })
        .Map<double>([](int x) { return x / 10.0; })
        .Subscribe(
            [](double value) { 
                Serial.print("Fluent - Scaled value: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent processing completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent processing error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void fromvector_aggregation_example() {
    Serial.println("=== FromVector Aggregation Example ===");
    
    std::vector<int> values = {1, 2, 3, 4, 5};
    
    // Traditional sum
    auto sum_obs = Sum(FromVector(values));
    sum_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional - Sum: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional sum completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional sum error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Fluent sum
    From(FromVector(values))
        .Sum()
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent - Sum: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent sum completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent sum error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("FromVector Source Examples");
    Serial.println("=========================");
    
    traditional_fromvector_example();
    fluent_fromvector_example();
    fromvector_with_operators_example();
    fromvector_aggregation_example();
}

void loop() {
    // Nothing to do in loop
}
