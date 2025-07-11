/**
 * Sum Operator Example
 * 
 * Demonstrates the Sum operator which calculates the sum of all
 * emitted numeric values.
 */

#include "../../include/micro-reactive.h"
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
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach - much cleaner
    Observable(Range(1, 10))
        .Filter([](int x) { return x % 2 == 1; })  // Odd numbers
        .Sum()
        .Subscribe([](int value) { 
            std::cout << "Fluent - Sum of odd numbers: " << value << std::endl; 
        });
    
    std::cout << std::endl;
}

void sum_with_transformation_example() {
    std::cout << "=== Sum with Transformation Example ===" << std::endl;
    
    // Sum of squares
    Observable(Range(1, 5))
        .Map<int>([](int x) { return x * x; })
        .Sum()
        .Subscribe([](int value) {
            std::cout << "Sum of squares (1² + 2² + 3² + 4² + 5²): " << value << std::endl;
        });
    
    std::cout << std::endl;
    
    // Sum after doubling
    Observable(Range(1, 4))
        .Map<int>([](int x) { return x * 2; })
        .Sum()
        .Subscribe([](int value) {
            std::cout << "Sum after doubling (2 + 4 + 6 + 8): " << value << std::endl;
        });
    
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
        }
    ));
    
    // Fluent approach
    Observable(subject)
        .Sum()
        .Subscribe([](int value) { 
            std::cout << "Fluent - Subject sum: " << value << std::endl; 
        },
        []() { 
            std::cout << "Fluent subject sum completed" << std::endl; 
        });
    
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
        .Subscribe([](int value) {
            std::cout << "Sum of single value (42): " << value << std::endl;
        });
    
    std::cout << std::endl;
}

void sum_with_take_example() {
    std::cout << "=== Sum with Take Example ===" << std::endl;
    
    // Sum only the first few values
    Observable(Range(1, 100))
        .Take(5)  // Take only first 5 values (1,2,3,4,5)
        .Sum()
        .Subscribe([](int value) {
            std::cout << "Sum of first 5 numbers: " << value << std::endl;
        });
    
    std::cout << std::endl;
    
    // Compare with taking different amounts
    Observable(Range(1, 100))
        .Take(10)  // Take first 10 values
        .Sum()
        .Subscribe([](int value) {
            std::cout << "Sum of first 10 numbers: " << value << std::endl;
        });
    
    std::cout << std::endl;
}

void sum_performance_example() {
    std::cout << "=== Sum Performance Example ===" << std::endl;
    
    // Create a larger sequence to demonstrate efficiency
    std::vector<int> large_sequence;
    for (int i = 1; i <= 1000; ++i) {
        large_sequence.push_back(i);
    }
    
    std::cout << "Calculating sum of numbers 1-1000..." << std::endl;
    
    Observable(FromVector(large_sequence))
        .Sum()
        .Subscribe([](int value) {
            std::cout << "Sum of 1-1000: " << value << std::endl;
            std::cout << "Expected: " << (1000 * 1001 / 2) << " (using formula n*(n+1)/2)" << std::endl;
        });
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "Sum Operator Examples" << std::endl;
    std::cout << "====================" << std::endl;
    
    traditional_sum_example();
    fluent_sum_example();
    sum_with_vector_example();
    sum_with_filter_example();
    sum_with_transformation_example();
    sum_with_subject_example();
    sum_empty_sequence_example();
    sum_single_value_example();
    sum_with_take_example();
    sum_performance_example();
}

void loop() {
    // Nothing to do in loop
}
