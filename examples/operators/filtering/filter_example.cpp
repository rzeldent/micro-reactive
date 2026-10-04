#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * Filter Operator Example
 * 
 * Demonstrates the Filter operator which only emits values
 * that satisfy a predicate function.
 */

void traditional_filter_example() {
    Serial.println("=== Traditional Filter Example ===");
    
    // Create a range of numbers
    auto range_observable = Range(1, 10);
    
    // Filter to only even numbers
    auto filtered_observable = Filter(range_observable, [](int x) {
        return x % 2 == 0;
    });
    
    // Subscribe to see the results
    auto observer = CreateObserver<int>(
        [](int value) { 
            Serial.print("Even number: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Filter operation completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Error: "); 
            Serial.println(e.what()); 
        }
    );
    
    auto subscription = filtered_observable->Subscribe(observer);
    
    Serial.println();
}

void fluent_filter_example() {
    Serial.println("=== Fluent Filter Example ===");
    
    // Use fluent interface to filter numbers greater than 5
    From(Range(1, 10))
        .Filter([](int x) {
            return x > 5;
        })
        .Subscribe(
            [](int value) { 
                Serial.print("Filtered value: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent filter completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void filter_with_strings_example() {
    Serial.println("=== Filter with Strings Example ===");
    
    std::vector<std::string> words = {"apple", "banana", "cherry", "date", "elderberry"};
    
    // Traditional approach - filter strings longer than 5 characters
    auto vector_obs = FromVector(words);
    auto filtered_obs = Filter(vector_obs, [](const std::string& s) {
        return s.length() > 5;
    });
    
    filtered_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            Serial.print("Traditional - Long word: "); 
            Serial.println(value.c_str()); 
        },
        []() { 
            Serial.println("Traditional string filter completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional string filter error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach - filter strings starting with 'b' or 'c'
    From(FromVector(words))
        .Filter([](const std::string& s) {
            return s[0] == 'b' || s[0] == 'c';
        })
        .Subscribe(
            [](const std::string& value) { 
                Serial.print("Fluent - b/c word: "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Fluent string filter completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent string filter error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void filter_with_map_example() {
    Serial.println("=== Filter with Map Example ===");
    
    // Traditional approach - map then filter
    auto range_obs = Range(1, 10);
    auto squared_obs = Map<int, int>(range_obs, [](int x) { return x * x; });
    auto filtered_obs = Filter(squared_obs, [](int x) {
        return x > 25;
    });
    
    filtered_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional - Squared > 25: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional map+filter completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional map+filter error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach - chain map and filter
    From(Range(1, 10))
        .Map<int>([](int x) { return x * x; })
        .Filter([](int x) {
            return x > 25;
        })
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent - Squared > 25: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent map+filter completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent map+filter error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void filter_complex_predicate_example() {
    Serial.println("=== Filter Complex Predicate Example ===");
    
    std::vector<int> numbers = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    
    // Complex predicate: prime numbers
    auto is_prime = [](int n) {
        if (n < 2) return false;
        for (int i = 2; i * i <= n; ++i) {
            if (n % i == 0) return false;
        }
        return true;
    };
    
    From(FromVector(numbers))
        .Filter(is_prime)
        .Subscribe(
            [](int value) { 
                Serial.print("Prime: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Prime filter completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Prime filter error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void filter_with_subject_example() {
    Serial.println("=== Filter with Subject Example ===");
    
    auto subject = CreateSubject<int>();
    
    // Traditional approach
    auto filtered_subject = Filter(subject->AsObservable(), [](int x) {
        return x % 3 == 0; // multiples of 3
    });
    
    filtered_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional subject filter (x3): "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional subject filter completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional subject filter error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Fluent approach
    From(subject)
        .Filter([](int x) {
            return x % 5 == 0; // multiples of 5
        })
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent subject filter (x5): "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent subject filter completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent subject filter error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Push values through subject
    for (int i = 1; i <= 20; ++i) {
        subject->OnNext(i);
    }
    
    subject->OnCompleted();
    
    Serial.println();
}

void filter_no_matches_example() {
    Serial.println("=== Filter No Matches Example ===");
    
    // Filter that matches nothing
    From(Range(1, 5))
        .Filter([](int x) {
            return x > 100; // nothing matches
        })
        .Subscribe(
            [](int value) { 
                Serial.print("Should not see this: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("No matches filter completed (no values emitted)"); 
            },
            [](const std::exception& e) { 
                Serial.print("No matches filter error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void filter_all_matches_example() {
    Serial.println("=== Filter All Matches Example ===");
    
    // Filter that matches everything
    From(Range(1, 5))
        .Filter([](int x) {
            return x > 0; // everything matches
        })
        .Subscribe(
            [](int value) { 
                Serial.print("All matches: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("All matches filter completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("All matches filter error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void filter_chaining_example() {
    Serial.println("=== Filter Chaining Example ===");
    
    // Chain multiple filters
    From(Range(1, 20))
        .Filter([](int x) { return x % 2 == 0; })  // even
        .Filter([](int x) { return x > 5; })       // > 5
        .Filter([](int x) { return x < 15; })      // < 15
        .Subscribe(
            [](int value) { 
                Serial.print("Chained filter (even, >5, <15): "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Chained filter completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Chained filter error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("Filter Operator Examples");
    Serial.println("=======================");
    
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
