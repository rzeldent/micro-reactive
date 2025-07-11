/**
 * Do Operator Example
 * 
 * Demonstrates the Do operator which performs side effects
 * without modifying the emitted values (like tap in RxJS).
 */

#include "../../include/micro-reactive.h"
#include <iostream>
#include <vector>
#include <string>

using namespace rx;

void traditional_do_example() {
    std::cout << "=== Traditional Do Example ===" << std::endl;
    
    // Create a range of numbers
    auto range_observable = Range(1, 5);
    
    // Add side effect to log each value
    auto do_observable = Do(range_observable, [](int value) {
        std::cout << "  Side effect: Processing " << value << std::endl;
    });
    
    // Subscribe to see the results
    auto observer = CreateObserver<int>(
        [](int value) { 
            std::cout << "Final value: " << value << std::endl; 
        },
        []() { 
            std::cout << "Do operation completed" << std::endl; 
        },
        [](const std::exception& e) { 
            std::cout << "Error: " << e.what() << std::endl; 
        }
    );
    
    auto subscription = do_observable->Subscribe(observer);
    
    std::cout << std::endl;
}

void fluent_do_example() {
    std::cout << "=== Fluent Do Example ===" << std::endl;
    
    // Use fluent interface with Do operator
    Observable(Range(10, 13))
        .Do([](int value) {
            std::cout << "  Fluent side effect: Saw " << value << std::endl;
        })
        .Subscribe(
            [](int value) { 
                std::cout << "Fluent final: " << value << std::endl; 
            },
            []() { 
                std::cout << "Fluent do completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void do_for_logging_example() {
    std::cout << "=== Do for Logging Example ===" << std::endl;
    
    std::vector<std::string> words = {"hello", "reactive", "world", "programming"};
    
    // Traditional approach - add logging
    auto vector_obs = FromVector(words);
    auto logged_obs = Do(vector_obs, [](const std::string& word) {
        std::cout << "  LOG: Processing word '" << word << "' (length: " << word.length() << ")" << std::endl;
    });
    auto filtered_obs = Filter(logged_obs, [](const std::string& word) {
        return word.length() > 5;
    });
    
    filtered_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            std::cout << "Traditional - Long word: " << value << std::endl; 
        },
        []() { 
            std::cout << "Traditional logging completed" << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach - much cleaner
    Observable(FromVector(words))
        .Do([](const std::string& word) {
            std::cout << "  FLUENT LOG: Checking '" << word << "'" << std::endl;
        })
        .Filter([](const std::string& word) { return word.length() <= 5; })
        .Do([](const std::string& word) {
            std::cout << "  FLUENT LOG: '" << word << "' passed filter" << std::endl;
        })
        .Subscribe([](const std::string& value) { 
            std::cout << "Fluent - Short word: " << value << std::endl; 
        });
    
    std::cout << std::endl;
}

void do_with_transformation_example() {
    std::cout << "=== Do with Transformation Example ===" << std::endl;
    
    // Traditional approach - debug transformation pipeline
    auto range_obs = Range(1, 5);
    auto debug1_obs = Do(range_obs, [](int x) {
        std::cout << "  Before transformation: " << x << std::endl;
    });
    auto mapped_obs = Map<int, int>(debug1_obs, [](int x) { 
        return x * x; 
    });
    auto debug2_obs = Do(mapped_obs, [](int x) {
        std::cout << "  After transformation: " << x << std::endl;
    });
    
    debug2_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Traditional - Final squared: " << value << std::endl; 
        }
    ));
    
    std::cout << std::endl;
    
    // Fluent approach - easier to read pipeline
    Observable(Range(1, 5))
        .Do([](int x) {
            std::cout << "  Fluent before: " << x << std::endl;
        })
        .Map<int>([](int x) { return x * x * x; })  // Cube instead of square
        .Do([](int x) {
            std::cout << "  Fluent after: " << x << std::endl;
        })
        .Subscribe([](int value) { 
            std::cout << "Fluent - Final cubed: " << value << std::endl; 
        });
    
    std::cout << std::endl;
}

void do_for_metrics_example() {
    std::cout << "=== Do for Metrics Example ===" << std::endl;
    
    int total_processed = 0;
    int even_count = 0;
    int odd_count = 0;
    
    Observable(Range(1, 10))
        .Do([&total_processed](int x) {
            total_processed++;
            std::cout << "  Metrics: Processed item #" << total_processed << " (value: " << x << ")" << std::endl;
        })
        .Do([&even_count, &odd_count](int x) {
            if (x % 2 == 0) {
                even_count++;
                std::cout << "  Metrics: Even count now " << even_count << std::endl;
            } else {
                odd_count++;
                std::cout << "  Metrics: Odd count now " << odd_count << std::endl;
            }
        })
        .Subscribe(
            [](int value) { 
                // Main processing logic
                std::cout << "Processing: " << value << std::endl; 
            },
            [&total_processed, &even_count, &odd_count]() { 
                std::cout << "=== Final Metrics ===" << std::endl;
                std::cout << "Total processed: " << total_processed << std::endl;
                std::cout << "Even numbers: " << even_count << std::endl;
                std::cout << "Odd numbers: " << odd_count << std::endl;
            }
        );
    
    std::cout << std::endl;
}

void do_with_subject_example() {
    std::cout << "=== Do with Subject Example ===" << std::endl;
    
    auto subject = CreateSubject<int>();
    std::vector<int> side_effect_log;
    
    // Traditional approach
    auto do_obs = Do(subject->AsObservable(), [&side_effect_log](int value) {
        side_effect_log.push_back(value);
        std::cout << "  Traditional side effect: Logged " << value << std::endl;
    });
    
    do_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            std::cout << "Traditional - Subject value: " << value << std::endl; 
        }
    ));
    
    // Fluent approach
    Observable(subject)
        .Do([](int value) {
            std::cout << "  Fluent side effect: Observed " << value << std::endl;
        })
        .Subscribe([](int value) { 
            std::cout << "Fluent - Subject value: " << value << std::endl; 
        });
    
    // Push values through subject
    std::cout << "Pushing values: 100, 200, 300..." << std::endl;
    subject->OnNext(100);
    subject->OnNext(200);
    subject->OnNext(300);
    
    subject->OnCompleted();
    
    std::cout << "Side effect log contents: ";
    for (int val : side_effect_log) {
        std::cout << val << " ";
    }
    std::cout << std::endl;
    
    std::cout << std::endl;
}

void do_multiple_side_effects_example() {
    std::cout << "=== Multiple Do Side Effects Example ===" << std::endl;
    
    Observable(Range(1, 4))
        .Do([](int x) {
            std::cout << "  Side effect 1: Value is " << x << std::endl;
        })
        .Do([](int x) {
            std::cout << "  Side effect 2: Square would be " << (x * x) << std::endl;
        })
        .Do([](int x) {
            std::cout << "  Side effect 3: Is " << (x % 2 == 0 ? "even" : "odd") << std::endl;
        })
        .Subscribe([](int value) {
            std::cout << "Final processing: " << value << std::endl;
            std::cout << "  ---" << std::endl;
        });
    
    std::cout << std::endl;
}

void do_with_error_handling_example() {
    std::cout << "=== Do with Error Handling Example ===" << std::endl;
    
    std::vector<int> values = {1, 2, 0, 4, 5};  // 0 will cause division by zero
    
    Observable(FromVector(values))
        .Do([](int x) {
            std::cout << "  Processing value: " << x << std::endl;
        })
        .Map<double>([](int x) -> double {
            if (x == 0) {
                throw std::runtime_error("Division by zero!");
            }
            return 10.0 / x;
        })
        .Do([](double x) {
            std::cout << "  Successfully calculated: " << x << std::endl;
        })
        .Subscribe(
            [](double value) { 
                std::cout << "Result: " << value << std::endl; 
            },
            []() { 
                std::cout << "Do with error handling completed" << std::endl; 
            },
            [](const std::exception& e) { 
                std::cout << "Caught error: " << e.what() << std::endl; 
            }
        );
    
    std::cout << std::endl;
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    std::cout << "Do Operator Examples" << std::endl;
    std::cout << "===================" << std::endl;
    
    traditional_do_example();
    fluent_do_example();
    do_for_logging_example();
    do_with_transformation_example();
    do_for_metrics_example();
    do_with_subject_example();
    do_multiple_side_effects_example();
    do_with_error_handling_example();
}

void loop() {
    // Nothing to do in loop
}
