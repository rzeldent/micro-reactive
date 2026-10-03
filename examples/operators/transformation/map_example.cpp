/**
 * Map Operator Example
 * 
 * Demonstrates the Map operator which transforms each emitted value
 * using a provided function.
 */

#include <Arduino.h>
#include "../../include/micro-reactive.h"
#include <iostream>
#include <string>
#include <vector>

using namespace rx;

void traditional_map_example() {
    std::cout << "=== Traditional Map Example ===" << std::endl;
    
    // Create a range of numbers
    auto range_observable = Range(1, 5);
    
    // Map each number to its square
    auto mapped_observable = Map<int, int>(range_observable, [](int x) {
        return x * x;
    });
    
    // Subscribe to see the results
    auto observer = CreateObserver<int>(
        [](int value) { 
            std::cout << "Squared value: " << value << std::endl; 
        },
        []() { 
            std::cout << "Map operation completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Error: " << e.what() << std::endl; 
        }
    );
    
    auto subscription = mapped_observable->Subscribe(observer);
    
    std::cout << std::endl;
}

void fluent_map_example() {
    std::cout << "=== Fluent Map Example ===" << std::endl;
    
    // Use fluent interface to map numbers to strings
    From(Range(1, 5))
        .Map<std::string>([](int x) {
            return "Number: " + std::to_string(x);
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Mapped string: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent map completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void map_type_conversion_example() {
    std::cout << "=== Map Type Conversion Example ===" << std::endl;
    
    std::vector<int> numbers = {10, 20, 30, 40, 50};
    
    // Traditional approach - convert int to double and scale
    auto vector_obs = FromVector(numbers);
    auto scaled_obs = Map<int, double>(vector_obs, [](int x) {
        return x / 10.0;
    });
    
    scaled_obs->Subscribe(CreateObserver<double>(
        [](double value) { 
            std::cout << "Traditional - Scaled: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional scaling completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional scaling error: " << e.what() << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach - convert to percentage strings
    From(FromVector(numbers))
        .Map<std::string>([](int x) {
            return std::to_string(x) + "%";
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Fluent - Percentage: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent percentage conversion completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent percentage conversion error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void map_chaining_example() {
    std::cout << "=== Map Chaining Example ===" << std::endl;
    
    // Traditional approach - chain multiple maps
    auto range_obs = Range(1, 5);
    auto doubled_obs = Map<int, int>(range_obs, [](int x) { return x * 2; });
    auto stringified_obs = Map<int, std::string>(doubled_obs, [](int x) { 
        return "Doubled: " + std::to_string(x); 
    });
    auto upper_obs = Map<std::string, std::string>(stringified_obs, [](const std::string& s) {
        std::string result = s;
        std::transform(result.begin(), result.end(), result.begin(), ::toupper);
        return result;
    });
    
    upper_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Traditional chain: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional chain completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional chain error: " << e.what() << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach - much cleaner chaining
    From(Range(1, 5))
        .Map<int>([](int x) { return x * 3; })
        .Map<std::string>([](int x) { return "Tripled: " + std::to_string(x); })
        .Map<std::string>([](const std::string& s) {
            std::string result = s;
            std::transform(result.begin(), result.end(), result.begin(), ::tolower);
            return result;
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Fluent chain: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent chain completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent chain error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void map_with_subject_example() {
    std::cout << "=== Map with Subject Example ===" << std::endl;
    
    auto subject = CreateSubject<int>();
    
    // Traditional approach
    auto mapped_subject = Map<int, std::string>(subject->AsObservable(), [](int x) {
        if (x < 0) return "Negative: " + std::to_string(x);
        else if (x == 0) return std::string("Zero");
        else return "Positive: " + std::to_string(x);
    });
    
    mapped_subject->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Traditional subject map: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional subject map completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional subject map error: " << e.what() << std::endl; 
        }
    ));
    
    // Fluent approach
    From(subject)
        .Map<std::string>([](int x) {
            return "Absolute: " + std::to_string(std::abs(x));
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Fluent subject map: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent subject map completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent subject map error: " << e.what() << std::endl; 
            }
        );
    
    // Push values through subject
    subject->OnNext(-5);
    subject->OnNext(0);
    subject->OnNext(10);
    subject->OnNext(-3);
    
    subject->OnCompleted();
    
    std::cout << std::endl;
}

void map_complex_transformation_example() {
    std::cout << "=== Map Complex Transformation Example ===" << std::endl;
    
    // Create some sample data
    std::vector<int> scores = {85, 92, 78, 95, 88, 73, 91};
    
    // Complex transformation: score to grade with additional info
    From(FromVector(scores))
        .Map<std::string>([](int score) {
            std::string grade;
            if (score >= 90) grade = "A";
            else if (score >= 80) grade = "B";
            else if (score >= 70) grade = "C";
            else if (score >= 60) grade = "D";
            else grade = "F";
            
            return "Score: " + std::to_string(score) + " -> Grade: " + grade;
        })
        .Subscribe(
            [](const std::string& result) {
                std::cout << result << std::endl;
            },
            []() { 
                std::cout << "Complex transformation completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Complex transformation error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void map_error_handling_example() {
    std::cout << "=== Map Error Handling Example ===" << std::endl;
    
    std::vector<int> numbers = {1, 2, 0, 4, 5};
    
    // Map with potential division by zero
    From(FromVector(numbers))
        .Map<std::string>([](int x) -> std::string {
            if (x == 0) {
                throw std::runtime_error("Division by zero!");
            }
            return "1/" + std::to_string(x) + " = " + std::to_string(1.0/x);
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Result: " << value << std::endl; 
            },
            []() { 
                std::cout << "Map with error handling completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Caught error in map: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "Map Operator Examples" << std::endl;
    std::cout << "====================" << std::endl;
    
    traditional_map_example();
    fluent_map_example();
    map_type_conversion_example();
    map_chaining_example();
    map_with_subject_example();
    map_complex_transformation_example();
    map_error_handling_example();
}

void loop() {
    // Nothing to do in loop
}
