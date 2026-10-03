/**
 * Take Operator Example
 * 
 * Demonstrates the Take operator which emits only the first N values
 * from the source observable and then completes.
 */

#include <Arduino.h>
#include <micro-reactive.h>
#include <iostream>
#include <vector>

using namespace rx;

void traditional_take_example() {
    std::cout << "=== Traditional Take Example ===" << std::endl;
    
    // Create a range of numbers 1-10
    auto range_observable = Range(1, 10);
    
    // Take only the first 3 values
    auto take_observable = Take(range_observable, 3);
    
    // Subscribe to see the results
    auto observer = CreateObserver<int>(
        [](int value) { 
            std::cout << "Taken value: " << value << std::endl; 
        },
        []() { 
            std::cout << "Take operation completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Error: " << e.what() << std::endl; 
        }
    );
    
    auto subscription = take_observable->Subscribe(observer);
    
    std::cout << std::endl;
}

void fluent_take_example() {
    std::cout << "=== Fluent Take Example ===" << std::endl;
    
    // Use fluent interface to take first 5 values
    From(Range(1, 20))
        .Take(5)
        .Subscribe(
            [](int value) { 
                std::cout << "Fluent take: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent take completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void take_with_vector_example() {
    std::cout << "=== Take with Vector Example ===" << std::endl;
    
    std::vector<std::string> fruits = {"apple", "banana", "orange", "grape", "kiwi", "mango", "peach"};
    
    // Traditional approach - take first 3 fruits
    auto vector_obs = FromVector(fruits);
    auto take_obs = Take(vector_obs, 3);
    
    take_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Traditional - First fruit: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional take fruits completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional take fruits error: " << e.what() << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach - take first 4 fruits
    From(FromVector(fruits))
        .Take(4)
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Fluent - Selected fruit: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent take fruits completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent take fruits error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void take_with_transformation_example() {
    std::cout << "=== Take with Transformation Example ===" << std::endl;
    
    // Traditional approach - take then map
    auto range_obs = Range(1, 15);
    auto take_obs = Take(range_obs, 5);
    auto mapped_obs = Map<int, std::string>(take_obs, [](int x) { 
        return "First " + std::to_string(x) + " squared = " + std::to_string(x * x); 
    });
    
    mapped_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Traditional - " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional take+map completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional take+map error: " << e.what() << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach - much cleaner
    From(Range(1, 15))
        .Take(6)
        .Map<std::string>([](int x) { 
            return "Selected " + std::to_string(x) + " cubed = " + std::to_string(x * x * x); 
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Fluent - " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent take+map completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent take+map error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void take_with_filter_example() {
    std::cout << "=== Take with Filter Example ===" << std::endl;
    
    // Take first 3 even numbers from range 1-20
    From(Range(1, 20))
        .Filter([](int x) { return x % 2 == 0; })
        .Take(3)
        .Subscribe(
            [](int value) {
                std::cout << "First 3 even numbers: " << value << std::endl;
            },
            []() { 
                std::cout << "Filter then take completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Filter then take error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
    
    // Compare with: filter after take (different result)
    From(Range(1, 20))
        .Take(6)  // Take first 6 numbers (1,2,3,4,5,6)
        .Filter([](int x) { return x % 2 == 0; })  // Then filter even (2,4,6)
        .Subscribe(
            [](int value) {
                std::cout << "Even numbers from first 6: " << value << std::endl;
            },
            []() { 
                std::cout << "Take then filter completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Take then filter error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void take_with_subject_example() {
    std::cout << "=== Take with Subject Example ===" << std::endl;
    
    auto subject = CreateSubject<int>();
    
    // Traditional approach
    auto take_obs = Take(subject->AsObservable(), 3);
    
    take_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Traditional - Taken from subject: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional take from subject completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional take from subject error: " << e.what() << std::endl; 
        }
    ));
    
    // Fluent approach
    From(subject)
        .Take(2)
        .Subscribe(
            [](int value) { 
                std::cout << "Fluent - Taken from subject: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent take from subject completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent take from subject error: " << e.what() << std::endl; 
            }
        );
    
    // Push values through subject
    std::cout << "Pushing values to subject..." << std::endl;
    subject->OnNext(10);  // Both should receive this
    subject->OnNext(20);  // Both should receive this
    subject->OnNext(30);  // Only traditional should receive this (fluent completes after 2)
    subject->OnNext(40);  // Traditional completes after 3, so this won't be received
    
    subject->OnCompleted();
    
    std::cout << std::endl;
}

void take_zero_example() {
    std::cout << "=== Take Zero Example ===" << std::endl;
    
    // Taking 0 elements should complete immediately
    From(Range(1, 10))
        .Take(0)
        .Subscribe(
            [](int value) { 
                std::cout << "This should not print: " << value << std::endl; 
            },
            []() { 
                std::cout << "Take 0 completed immediately" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Take 0 error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void take_more_than_available_example() {
    std::cout << "=== Take More Than Available Example ===" << std::endl;
    
    std::vector<int> small_vector = {1, 2, 3};
    
    // Try to take 10 elements from a 3-element vector
    From(FromVector(small_vector))
        .Take(10)
        .Subscribe(
            [](int value) { 
                std::cout << "Available value: " << value << std::endl; 
            },
            []() { 
                std::cout << "Take completed (only 3 values available)" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Take more than available error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void take_chaining_example() {
    std::cout << "=== Take Chaining Example ===" << std::endl;
    
    // Multiple take operations (should take the minimum)
    From(Range(1, 100))
        .Take(10)   // First, take 10
        .Take(5)    // Then, take 5 from those 10
        .Subscribe(
            [](int value) {
                std::cout << "Chained take result: " << value << std::endl;
            },
            []() {
                std::cout << "Chained take completed (should have 5 values)" << std::endl;
            },
            [](const std::exception& e) { 
                std::cout << "Chained take error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "Take Operator Examples" << std::endl;
    std::cout << "=====================" << std::endl;
    
    traditional_take_example();
    fluent_take_example();
    take_with_vector_example();
    take_with_transformation_example();
    take_with_filter_example();
    take_with_subject_example();
    take_zero_example();
    take_more_than_available_example();
    take_chaining_example();
}

void loop() {
    // Nothing to do in loop
}
