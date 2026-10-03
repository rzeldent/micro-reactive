/**
 * Filter Operator Example
 * 
 * Demonstrates the Filter operator which only emits values
 * that satisfy a predicate function.
 */

#include <Arduino.h>
#include "../../include/micro-reactive.h"
#include <iostream>
#include <string>
#include <vector>

using namespace rx;

void traditional_filter_example() {
    std::cout << "=== Traditional Filter Example ===" << std::endl;
    
    // Create a range of numbers
    auto range_observable = Range(1, 10);
    
    // Filter to only even numbers
    auto filtered_observable = Filter(range_observable, [](int x) {
        return x % 2 == 0;
    });
    
    // Subscribe to see the results
    auto observer = CreateObserver<int>(
        [](int value) { 
            std::cout << "Even number: " << value << std::endl; 
        },
        []() { 
            std::cout << "Filter operation completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Error: " << e.what() << std::endl; 
        }
    );
    
    auto subscription = filtered_observable->Subscribe(observer);
    
    std::cout << std::endl;
}

void fluent_filter_example() {
    std::cout << "=== Fluent Filter Example ===" << std::endl;
    
    // Use fluent interface to filter numbers greater than 5
    From(Range(1, 10))
        .Filter([](int x) {
            return x > 5;
        })
        .Subscribe(
            [](int value) { 
                std::cout << "Number > 5: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent filter completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void filter_with_strings_example() {
    std::cout << "=== Filter with Strings Example ===" << std::endl;
    
    std::vector<std::string> words = {"hello", "world", "reactive", "programming", "filter", "example"};
    
    // Traditional approach - filter words with length > 5
    auto vector_obs = FromVector(words);
    auto long_words_obs = Filter(vector_obs, [](const std::string& word) {
        return word.length() > 5;
    });
    
    long_words_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Traditional - Long word: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional long words filter completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional long words filter error: " << e.what() << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach - filter words starting with 'r'
    From(FromVector(words))
        .Filter([](const std::string& word) {
            return !word.empty() && word[0] == 'r';
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Fluent - Word starting with 'r': " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent 'r' words filter completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent 'r' words filter error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void filter_with_map_example() {
    std::cout << "=== Filter with Map Example ===" << std::endl;
    
    // Traditional approach - filter then map
    auto range_obs = Range(1, 20);
    auto filtered_obs = Filter(range_obs, [](int x) { return x % 3 == 0; });
    auto mapped_obs = Map<int, std::string>(filtered_obs, [](int x) { 
        return "Multiple of 3: " + std::to_string(x); 
    });
    
    mapped_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Traditional - " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional filter+map completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional filter+map error: " << e.what() << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach - much cleaner
    From(Range(1, 20))
        .Filter([](int x) { return x % 5 == 0; })
        .Map<std::string>([](int x) { 
            return "Multiple of 5: " + std::to_string(x); 
        })
        .Subscribe(
            [](const std::string& value) { 
                std::cout << "Fluent - " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent filter+map completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent filter+map error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void filter_complex_predicate_example() {
    std::cout << "=== Filter Complex Predicate Example ===" << std::endl;
    
    std::vector<int> numbers = {1, 4, 9, 16, 25, 36, 49, 64, 81, 100};
    
    // Filter perfect squares that are also divisible by 4
    From(FromVector(numbers))
        .Filter([](int x) {
            // Check if it's a perfect square divisible by 4
            int root = static_cast<int>(std::sqrt(x));
            return (root * root == x) && (x % 4 == 0);
        })
        .Subscribe(
            [](int value) {
                std::cout << "Perfect square divisible by 4: " << value << std::endl;
            },
            []() { 
                std::cout << "Complex predicate filter completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Complex predicate filter error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void filter_with_subject_example() {
    std::cout << "=== Filter with Subject Example ===" << std::endl;
    
    auto subject = CreateSubject<int>();
    
    // Traditional approach
    auto positive_filter = Filter(subject->AsObservable(), [](int x) {
        return x > 0;
    });
    
    positive_filter->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Traditional - Positive: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional positive filter completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Traditional positive filter error: " << e.what() << std::endl; 
        }
    ));
    
    // Fluent approach
    From(subject)
        .Filter([](int x) { return x < 0; })
        .Subscribe(
            [](int value) { 
                std::cout << "Fluent - Negative: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent negative filter completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Fluent negative filter error: " << e.what() << std::endl; 
            }
        );
    
    // Push various values through subject
    subject->OnNext(5);
    subject->OnNext(-3);
    subject->OnNext(0);
    subject->OnNext(10);
    subject->OnNext(-7);
    subject->OnNext(2);
    
    subject->OnCompleted();
    
    std::cout << std::endl;
}

void filter_no_matches_example() {
    std::cout << "=== Filter No Matches Example ===" << std::endl;
    
    // Filter that matches nothing
    From(Range(1, 5))
        .Filter([](int x) {
            return x > 100; // No numbers 1-5 are > 100
        })
        .Subscribe(
            [](int value) { 
                std::cout << "This should not print: " << value << std::endl; 
            },
            []() { 
                std::cout << "Filter with no matches completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Filter with no matches error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void filter_all_matches_example() {
    std::cout << "=== Filter All Matches Example ===" << std::endl;
    
    // Filter that matches everything
    From(Range(1, 5))
        .Filter([](int x) {
            return true; // All numbers match
        })
        .Subscribe(
            [](int value) { 
                std::cout << "All numbers pass: " << value << std::endl; 
            },
            []() { 
                std::cout << "Filter allowing all completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Filter allowing all error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void filter_chaining_example() {
    std::cout << "=== Filter Chaining Example ===" << std::endl;
    
    // Chain multiple filters
    From(Range(1, 50))
        .Filter([](int x) { return x % 2 == 0; })     // Even numbers
        .Filter([](int x) { return x % 3 == 0; })     // Divisible by 3
        .Filter([](int x) { return x > 10; })         // Greater than 10
        .Subscribe(
            [](int value) {
                std::cout << "Even, divisible by 3, and > 10: " << value << std::endl;
            },
            []() { 
                std::cout << "Filter chaining completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Filter chaining error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "Filter Operator Examples" << std::endl;
    std::cout << "=======================" << std::endl;
    
    traditional_filter_example();
    fluent_filter_example();
    filter_with_strings_example();
    filter_with_map_example();
    filter_complex_predicate_example();
    filter_with_subject_example();
    filter_no_matches_example();
    filter_all_matches_example();
    filter_chaining_example();
}

void loop() {
    // Nothing to do in loop
}
