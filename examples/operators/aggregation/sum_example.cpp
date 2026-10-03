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

 #include <Arduino.h>
#include "../../../include/micro-reactive.h"
#include <iostream>
#include <vector>

using namespace rx;

void traditional_sum_example() {
    std::cout << "=== Traditional Sum Example ===" << std::endl;
    
    // Create a range of numbers
    auto range_observable = Range(1, 5);
    
    // Sum all the values
    auto sum_observable = Sum(range_observable);
    
    // Subscribe to see the result
    auto observer = CreateObserver<int>(
        [](int value) { 
            std::cout << "Sum result: " << value << std::endl; 
        },
        []() { 
            std::cout << "Sum operation completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Error: " << e.what() << std::endl; 
        }
    );
    
    auto subscription = sum_observable->Subscribe(observer);
    
    std::cout << std::endl;
}

void fluent_sum_example() {
    std::cout << "=== Fluent Sum Example ===" << std::endl;
    
    // Use fluent interface to sum numbers
    Observable(Range(10, 15))
        .Sum()
        .Subscribe(
            [](int value) { 
                std::cout << "Fluent sum result: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent sum completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void sum_with_vector_example() {
    std::cout << "=== Sum with Vector Example ===" << std::endl;
    
    std::vector<int> numbers = {2, 4, 6, 8, 10};
    
    // Traditional approach
    auto vector_obs = FromVector(numbers);
    auto sum_obs = Sum(vector_obs);
    
    sum_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Traditional - Vector sum: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional vector sum completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional vector sum error: " << e.what() << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach
    Observable(FromVector(numbers))
        .Sum()
        .Subscribe(
            [](int value) { 
                std::cout << "Fluent - Vector sum: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent vector sum completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent vector sum error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void sum_with_filter_example() {
    std::cout << "=== Sum with Filter Example ===" << std::endl;
    
    // Traditional approach - filter then sum
    auto range_obs = Range(1, 10);
    auto filtered_obs = Filter(range_obs, [](int x) { return x % 2 == 0; });
    auto sum_obs = Sum(filtered_obs);
    
    sum_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Traditional - Sum of even numbers: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional filter sum completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional filter sum error: " << e.what() << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach - much cleaner
    Observable(Range(1, 10))
        .Filter([](int x) { return x % 2 == 1; })  // Odd numbers
        .Sum()
        .Subscribe(
            [](int value) { 
                std::cout << "Fluent - Sum of odd numbers: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent filter sum completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent filter sum error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void sum_with_transformation_example() {
    std::cout << "=== Sum with Transformation Example ===" << std::endl;
    
    // Sum of squares
    Observable(Range(1, 5))
        .Map<int>([](int x) { return x * x; })
        .Sum()
        .Subscribe(
            [](int value) {
                std::cout << "Sum of squares (1² + 2² + 3² + 4² + 5²): " << value << std::endl;
            },
            []() {
                std::cout << "Sum of squares completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Sum of squares error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
    
    // Sum after doubling
    Observable(Range(1, 4))
        .Map<int>([](int x) { return x * 2; })
        .Sum()
        .Subscribe(
            [](int value) {
                std::cout << "Sum after doubling (2 + 4 + 6 + 8): " << value << std::endl;
            },
            []() {
                std::cout << "Sum after doubling completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Sum after doubling error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
}

void sum_with_subject_example() {
    std::cout << "=== Sum with Subject Example ===" << std::endl;
    
    auto subject = CreateSubject<int>();
    
    // Traditional approach
    auto sum_obs = Sum(subject->AsObservable());
    
    sum_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Traditional - Subject sum: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional subject sum completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional subject sum error: " << e.what() << std::endl; 
        }
    ));
    
    // Fluent approach
    Observable(subject)
        .Sum()
        .Subscribe(
            [](int value) { 
                std::cout << "Fluent - Subject sum: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent subject sum completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent subject sum error: " << e.what() << std::endl; 
            }
        );
    
    // Push values through subject
    std::cout << "Pushing values: 5, 10, 15..." << std::endl;
    subject->OnNext(5);
    subject->OnNext(10);
    subject->OnNext(15);
    
    // Complete to trigger sum calculation
    subject->OnCompleted();
    
    std::cout << std::endl;
}

void sum_empty_sequence_example() {
    std::cout << "=== Sum Empty Sequence Example ===" << std::endl;
    
    // Sum of empty sequence should be 0
    Observable(Empty<int>())
        .Sum()
        .Subscribe(
            [](int value) { 
                std::cout << "Sum of empty sequence: " << value << std::endl; 
            },
            []() { 
                std::cout << "Empty sum completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Empty sum error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void sum_single_value_example() {
    std::cout << "=== Sum Single Value Example ===" << std::endl;
    
    std::vector<int> single_value = {42};
    
    // Sum of single value should be the value itself
    Observable(FromVector(single_value))
        .Sum()
        .Subscribe(
            [](int value) {
                std::cout << "Sum of single value (42): " << value << std::endl;
            },
            []() {
                std::cout << "Single value sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Single value sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
}

void sum_with_take_example() {
    std::cout << "=== Sum with Take Example ===" << std::endl;
    
    // Sum only the first few values
    Observable(Range(1, 100))
        .Take(5)  // Take only first 5 values (1,2,3,4,5)
        .Sum()
        .Subscribe(
            [](int value) {
                std::cout << "Sum of first 5 numbers: " << value << std::endl;
            },
            []() {
                std::cout << "Take 5 sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Take 5 sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
    
    // Compare with taking different amounts
    Observable(Range(1, 100))
        .Take(10)  // Take first 10 values
        .Sum()
        .Subscribe(
            [](int value) {
                std::cout << "Sum of first 10 numbers: " << value << std::endl;
            },
            []() {
                std::cout << "Take 10 sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Take 10 sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
}

void sum_performance_example() {
    std::cout << "=== Sum Performance Example ===" << std::endl;
    
    // ESP32-friendly sequence size (avoid large memory allocation)
    const int sequence_size = 100; // Reduced for embedded systems
    std::vector<int> large_sequence;
    large_sequence.reserve(sequence_size); // Pre-allocate memory
    
    for (int i = 1; i <= sequence_size; ++i) {
        large_sequence.push_back(i);
    }
    
    std::cout << "Calculating sum of numbers 1-" << sequence_size << " (ESP32 optimized)..." << std::endl;
    
    Observable(FromVector(large_sequence))
        .Sum()
        .Subscribe(
            [sequence_size](int value) {
                std::cout << "Sum of 1-" << sequence_size << ": " << value << std::endl;
                std::cout << "Expected: " << (sequence_size * (sequence_size + 1) / 2) << " (using formula n*(n+1)/2)" << std::endl;
            },
            []() {
                std::cout << "Performance sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Performance sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
    
    // Memory-efficient approach using Range instead of vector
    std::cout << "Memory-efficient approach using Range..." << std::endl;
    Observable(Range(1, sequence_size))
        .Sum()
        .Subscribe(
            [sequence_size](int value) {
                std::cout << "Range-based sum 1-" << sequence_size << ": " << value << std::endl;
            },
            []() {
                std::cout << "Range-based sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Range-based sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
}

void sum_error_handling_example() {
    std::cout << "=== Sum Error Handling Example ===" << std::endl;
    
    auto subject = CreateSubject<int>();
    
    // Sum should stop and propagate error when source errors
    Observable(subject)
        .Sum()
        .Subscribe(
            [](int value) { 
                std::cout << "This should not be called: " << value << std::endl; 
            },
            []() { 
                std::cout << "This should not complete after error" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error caught in sum: " << e.what() << std::endl; 
            }
        );
    
    // Push some values then error
    subject->OnNext(10);
    subject->OnNext(20);
    subject->OnError(std::runtime_error("Sum calculation failed"));
    
    // These should not be processed
    subject->OnNext(30);
    subject->OnCompleted();
    
    std::cout << std::endl;
}

void sum_error_propagation_example() {
    std::cout << "=== Sum Error Propagation Example ===" << std::endl;
    
    // Demonstrate how errors propagate through operator chains
    auto subject = CreateSubject<int>();
    
    Observable(subject)
        .Map<int>([](int x) {
            if (x == 666) {
                throw std::runtime_error("Evil number detected in Map!");
            }
            return x * 2;
        })
        .Filter([](int x) { return x > 0; })
        .Sum()
        .Subscribe(
            [](int value) { 
                std::cout << "This should not be reached due to error: " << value << std::endl; 
            },
            []() { 
                std::cout << "This should not complete due to error" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error propagated through chain: " << e.what() << std::endl; 
            }
        );
    
    // Push some normal values first
    std::cout << "Pushing normal values: 1, 2, 3..." << std::endl;
    subject->OnNext(1);
    subject->OnNext(2);
    subject->OnNext(3);
    
    // Push the problematic value that will cause error in Map
    std::cout << "Pushing evil number 666..." << std::endl;
    subject->OnNext(666);
    
    // These should not be processed after the error
    subject->OnNext(4);
    subject->OnCompleted();
    
    std::cout << std::endl;
}

void sum_robust_error_handling_example() {
    std::cout << "=== Sum Robust Error Handling Example ===" << std::endl;
    
    // Example showing how to handle potential division by zero in calculations
    std::vector<int> data_with_potential_issues = {10, 20, 0, 30, 40};
    
    Observable(FromVector(data_with_potential_issues))
        .Map<double>([](int x) -> double {
            if (x == 0) {
                std::cout << "Warning: Zero value encountered, treating as 1 to avoid issues" << std::endl;
                return 1.0;
            }
            return static_cast<double>(x);
        })
        .Sum()
        .Subscribe(
            [](double value) { 
                std::cout << "Robust sum with zero handling: " << value << std::endl; 
            },
            []() { 
                std::cout << "Robust sum completed successfully" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Unexpected error in robust sum: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void sum_overflow_example() {
    std::cout << "=== Sum Overflow Example ===" << std::endl;
    
    // Demonstrate potential overflow with large numbers
    std::vector<int> large_numbers = {2000000000, 1000000000, 500000000};
    
    std::cout << "Summing large numbers that might overflow..." << std::endl;
    Observable(FromVector(large_numbers))
        .Sum()
        .Subscribe(
            [](int value) { 
                std::cout << "Sum result (check for overflow): " << value << std::endl; 
            },
            []() { 
                std::cout << "Large number sum completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error in large sum: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void sum_negative_numbers_example() {
    std::cout << "=== Sum Negative Numbers Example ===" << std::endl;
    
    std::vector<int> mixed_numbers = {-5, 10, -3, 8, -2};
    
    Observable(FromVector(mixed_numbers))
        .Sum()
        .Subscribe(
            [](int value) {
                std::cout << "Sum of mixed positive/negative: " << value << std::endl;
            },
            []() {
                std::cout << "Mixed numbers sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Mixed numbers sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
    
    // All negative numbers
    std::vector<int> negative_numbers = {-1, -2, -3, -4, -5};
    
    Observable(FromVector(negative_numbers))
        .Sum()
        .Subscribe(
            [](int value) {
                std::cout << "Sum of all negative numbers: " << value << std::endl;
            },
            []() {
                std::cout << "All negative numbers sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "All negative numbers sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
}

void sum_zero_values_example() {
    std::cout << "=== Sum Zero Values Example ===" << std::endl;
    
    std::vector<int> with_zeros = {0, 5, 0, 10, 0, 15};
    
    Observable(FromVector(with_zeros))
        .Sum()
        .Subscribe(
            [](int value) {
                std::cout << "Sum with zeros included: " << value << std::endl;
            },
            []() {
                std::cout << "Sum with zeros completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Sum with zeros error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
    
    // All zeros
    std::vector<int> all_zeros = {0, 0, 0, 0};
    
    Observable(FromVector(all_zeros))
        .Sum()
        .Subscribe(
            [](int value) {
                std::cout << "Sum of all zeros: " << value << std::endl;
            },
            []() {
                std::cout << "All zeros sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "All zeros sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
}

void sum_different_types_example() {
    std::cout << "=== Sum Different Types Example ===" << std::endl;
    
    // Float sum
    std::vector<float> float_numbers = {1.5f, 2.5f, 3.5f, 4.5f};
    
    Observable(FromVector(float_numbers))
        .Sum()
        .Subscribe(
            [](float value) {
                std::cout << "Sum of floats: " << value << std::endl;
            },
            []() {
                std::cout << "Float sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Float sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
    
    // Double sum for higher precision
    std::vector<double> double_numbers = {0.1, 0.2, 0.3, 0.4, 0.5};
    
    Observable(FromVector(double_numbers))
        .Sum()
        .Subscribe(
            [](double value) {
                std::cout << "Sum of doubles (precision): " << value << std::endl;
            },
            []() {
                std::cout << "Double sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Double sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
}

void sum_real_world_examples() {
    std::cout << "=== Sum Real-World Examples ===" << std::endl;
    
    // Sensor data aggregation (common in ESP32 projects)
    std::cout << "--- Sensor Reading Sum ---" << std::endl;
    std::vector<int> temperature_readings = {23, 24, 25, 23, 26, 24, 25};
    
    Observable(FromVector(temperature_readings))
        .Sum()
        .Subscribe(
            [](int total_temp) {
                int avg_temp = total_temp / 7; // Simple average
                std::cout << "Total temperature readings: " << total_temp << "°C" << std::endl;
                std::cout << "Average temperature: " << avg_temp << "°C" << std::endl;
            },
            []() {
                std::cout << "Temperature sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Temperature sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
    
    // Battery level monitoring
    std::cout << "--- Battery Consumption Sum ---" << std::endl;
    std::vector<int> power_consumption_mah = {50, 45, 60, 55, 40, 52, 48};
    
    Observable(FromVector(power_consumption_mah))
        .Sum()
        .Subscribe(
            [](int total_consumption) {
                std::cout << "Total power consumption: " << total_consumption << " mAh" << std::endl;
                if (total_consumption > 300) {
                    std::cout << "Warning: High power consumption detected!" << std::endl;
                }
            },
            []() {
                std::cout << "Battery consumption sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Battery consumption sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
    
    // Data packet size calculation
    std::cout << "--- Network Packet Size Sum ---" << std::endl;
    std::vector<int> packet_sizes = {128, 256, 64, 512, 128, 256};
    
    Observable(FromVector(packet_sizes))
        .Sum()
        .Subscribe(
            [](int total_bytes) {
                std::cout << "Total data transmitted: " << total_bytes << " bytes" << std::endl;
                std::cout << "Total data transmitted: " << (total_bytes / 1024.0) << " KB" << std::endl;
            },
            []() {
                std::cout << "Network packet sum completed" << std::endl;
            },
            [](const std::exception& e) {
                std::cout << "Network packet sum error: " << e.what() << std::endl;
            }
        );
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "Sum Operator Examples" << std::endl;
    std::cout << "====================" << std::endl;
    
    // Basic usage examples
    traditional_sum_example();
    fluent_sum_example();
    sum_with_vector_example();
    
    // Operator combination examples
    sum_with_filter_example();
    sum_with_transformation_example();
    sum_with_take_example();
    
    // Subject integration
    sum_with_subject_example();
    
    // Edge cases and basic error handling
    sum_empty_sequence_example();
    sum_single_value_example();
    sum_negative_numbers_example();
    sum_zero_values_example();
    
    // Advanced error handling examples
    sum_error_handling_example();
    sum_error_propagation_example();
    sum_robust_error_handling_example();
    sum_overflow_example();
    
    // Type safety and performance
    sum_different_types_example();
    sum_performance_example();
    
    // Real-world usage
    sum_real_world_examples();
    
    std::cout << "All Sum operator examples completed!" << std::endl;
}

void loop() {
    // Nothing to do in loop
}
