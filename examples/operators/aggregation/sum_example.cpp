#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * Sum Operator Example
 * 
 * Demonstrates the Sum operator which calculates the sum of all
 * emitted numeric values. The Sum operator is an aggregation operator
 * that accumulates values and emits the final result when the source
 * observable completes.
 * 
 * Key behaviors:
 * - Accumulates all emitted values
 * - Emits single result when source completes
 * - Returns 0 for empty sequences (default identity)
 * - Propagates errors immediately
 * - Works with any numeric type (int, float, double)
 * 
 * ESP32 Considerations:
 * - Uses efficient memory patterns
 * - Handles overflow scenarios
 * - Optimized for embedded constraints
 * 
 * Examples included:
 * - Basic traditional and fluent usage
 * - Operator combinations (filter, map, take)
 * - Subject integration
 * - Error handling and edge cases
 * - Different numeric types
 * - Real-world ESP32 use cases
 */

void traditional_sum_example() {
    Serial.println("=== Traditional Sum Example ===");
    
    // Create a range of numbers
    auto range_observable = Range(1, 5);
    
    // Sum all the values
    auto sum_observable = Sum(range_observable);
    
    // Subscribe to see the result
    auto observer = CreateObserver<int>(
        [](int value) { 
            Serial.print("Sum result: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Sum operation completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Error: "); 
            Serial.println(e.what()); 
        }
    );
    
    auto subscription = sum_observable->Subscribe(observer);
    
    Serial.println();
}

void fluent_sum_example() {
    Serial.println("=== Fluent Sum Example ===");
    
    // Use fluent interface to sum numbers
    From(Range(1, 5))
        .Sum()
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent sum result: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent sum completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void sum_with_filter_example() {
    Serial.println("=== Sum with Filter Example ===");
    
    // Traditional approach - filter then sum
    auto range_obs = Range(1, 10);
    auto filtered_obs = Filter(range_obs, [](int x) { return x % 2 == 0; }); // even numbers
    auto sum_obs = Sum(filtered_obs);
    
    sum_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional - Sum of evens: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional filter+sum completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional filter+sum error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach - chain filter and sum
    From(Range(1, 10))
        .Filter([](int x) { return x % 2 == 0; })
        .Sum()
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent - Sum of evens: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent filter+sum completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent filter+sum error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void sum_with_map_example() {
    Serial.println("=== Sum with Map Example ===");
    
    // Traditional approach - map then sum
    auto range_obs = Range(1, 5);
    auto squared_obs = Map<int, int>(range_obs, [](int x) { return x * x; });
    auto sum_obs = Sum(squared_obs);
    
    sum_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional - Sum of squares: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional map+sum completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional map+sum error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach - chain map and sum
    From(Range(1, 5))
        .Map<int>([](int x) { return x * x; })
        .Sum()
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent - Sum of squares: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent map+sum completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent map+sum error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void sum_with_take_example() {
    Serial.println("=== Sum with Take Example ===");
    
    // Traditional approach - take then sum
    auto range_obs = Range(1, 10);
    auto taken_obs = Take(range_obs, 5);
    auto sum_obs = Sum(taken_obs);
    
    sum_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional - Sum of first 5: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional take+sum completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional take+sum error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach - chain take and sum
    From(Range(1, 10))
        .Take(5)
        .Sum()
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent - Sum of first 5: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent take+sum completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent take+sum error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void sum_with_subject_example() {
    Serial.println("=== Sum with Subject Example ===");
    
    auto subject = CreateSubject<int>();
    
    // Traditional approach
    auto sum_subject = Sum(subject->AsObservable());
    
    sum_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional subject sum: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional subject sum completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional subject sum error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Fluent approach
    From(subject)
        .Sum()
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent subject sum: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent subject sum completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent subject sum error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Push values through subject
    for (int i = 1; i <= 5; ++i) {
        subject->OnNext(i);
    }
    
    subject->OnCompleted();
    
    Serial.println();
}

void sum_empty_example() {
    Serial.println("=== Sum Empty Example ===");
    
    // Sum of empty sequence
    auto empty_obs = Empty<int>();
    auto sum_obs = Sum(empty_obs);
    
    sum_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Sum of empty: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Empty sum completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Empty sum error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
}

void sum_double_example() {
    Serial.println("=== Sum Double Example ===");
    
    std::vector<double> values = {1.5, 2.5, 3.5, 4.5};
    
    // Traditional approach
    auto vector_obs = FromVector(values);
    auto sum_obs = Sum(vector_obs);
    
    sum_obs->Subscribe(CreateObserver<double>(
        [](double value) { 
            Serial.print("Traditional - Sum of doubles: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional double sum completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional double sum error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach
    From(FromVector(values))
        .Sum()
        .Subscribe(
            [](double value) { 
                Serial.print("Fluent - Sum of doubles: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent double sum completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent double sum error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void sum_esp32_sensor_example() {
    Serial.println("=== ESP32 Sensor Sum Example ===");
    
    // Simulate sensor readings
    std::vector<int> sensor_readings = {1024, 1030, 1028, 1032, 1026, 1031, 1029, 1027};
    
    // Calculate average using sum and count
    auto readings_obs = FromVector(sensor_readings);
    auto sum_obs = Sum(readings_obs);
    
    sum_obs->Subscribe(CreateObserver<int>(
        [](int sum) { 
            Serial.print("Sum of sensor readings: "); 
            Serial.println(sum);
            Serial.print("Average: "); 
            Serial.println(sum / 8.0);
        },
        []() { 
            Serial.println("Sensor sum completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Sensor sum error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
}

void sum_overflow_example() {
    Serial.println("=== Sum Overflow Example ===");
    
    // Large numbers that might overflow
    std::vector<long> large_numbers = {1000000, 2000000, 3000000};
    
    From(FromVector(large_numbers))
        .Sum()
        .Subscribe(
            [](long value) { 
                Serial.print("Sum of large numbers: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Large number sum completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Large number sum error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("Sum Operator Examples");
    Serial.println("=====================");
    
    traditional_sum_example();
    fluent_sum_example();
    sum_with_filter_example();
    sum_with_map_example();
    sum_with_take_example();
    sum_with_subject_example();
    sum_empty_example();
    sum_double_example();
    sum_esp32_sensor_example();
    sum_overflow_example();
}

void loop() {
    // Nothing to do in loop
}
