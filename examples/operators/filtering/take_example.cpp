#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * Take Operator Example
 * 
 * Demonstrates the Take operator which emits only the first N values
 * from the source observable and then completes.
 */

void traditional_take_example() {
    Serial.println("=== Traditional Take Example ===");
    
    // Create a range of numbers 1-10
    auto range_observable = Range(1, 10);
    
    // Take only the first 3 values
    auto taken_observable = Take(range_observable, 3);
    
    // Subscribe to see the results
    auto observer = CreateObserver<int>(
        [](int value) { 
            Serial.print("Taken value: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Take operation completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Error: "); 
            Serial.println(e.what()); 
        }
    );
    
    auto subscription = taken_observable->Subscribe(observer);
    
    Serial.println();
}

void fluent_take_example() {
    Serial.println("=== Fluent Take Example ===");
    
    // Use fluent interface to take first 5 values
    From(Range(1, 10))
        .Take(5)
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent taken: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent take completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void take_with_strings_example() {
    Serial.println("=== Take with Strings Example ===");
    
    std::vector<std::string> words = {"first", "second", "third", "fourth", "fifth", "sixth"};
    
    // Traditional approach
    auto vector_obs = FromVector(words);
    auto taken_obs = Take(vector_obs, 3);
    
    taken_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            Serial.print("Traditional - Taken: "); 
            Serial.println(value.c_str()); 
        },
        []() { 
            Serial.println("Traditional take completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional take error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach
    From(FromVector(words))
        .Take(4)
        .Subscribe(
            [](const std::string& value) { 
                Serial.print("Fluent - Taken: "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Fluent take completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent take error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void take_with_map_example() {
    Serial.println("=== Take with Map Example ===");
    
    // Traditional approach - map then take
    auto range_obs = Range(1, 10);
    auto squared_obs = Map<int, int>(range_obs, [](int x) { return x * x; });
    auto taken_obs = Take(squared_obs, 4);
    
    taken_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional - Squared taken: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional map+take completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional map+take error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach - chain map and take
    From(Range(1, 10))
        .Map<int>([](int x) { return x * x; })
        .Take(4)
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent - Squared taken: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent map+take completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent map+take error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void take_more_than_available_example() {
    Serial.println("=== Take More Than Available Example ===");
    
    // Take more values than available
    From(Range(1, 3))
        .Take(10)
        .Subscribe(
            [](int value) { 
                Serial.print("Taken (more than available): "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Take more than available completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Take more than available error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void take_zero_example() {
    Serial.println("=== Take Zero Example ===");
    
    // Take zero values
    From(Range(1, 5))
        .Take(0)
        .Subscribe(
            [](int value) { 
                Serial.print("Should not see this: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Take zero completed (no values emitted)"); 
            },
            [](const std::exception& e) { 
                Serial.print("Take zero error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void take_with_subject_example() {
    Serial.println("=== Take with Subject Example ===");
    
    auto subject = CreateSubject<int>();
    
    // Traditional approach
    auto taken_subject = Take(subject->AsObservable(), 3);
    
    taken_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional subject take (3): "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional subject take completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional subject take error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Fluent approach
    From(subject)
        .Take(2)
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent subject take (2): "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent subject take completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent subject take error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Push values through subject
    for (int i = 1; i <= 10; ++i) {
        subject->OnNext(i);
    }
    
    subject->OnCompleted();
    
    Serial.println();
}

void take_chaining_example() {
    Serial.println("=== Take Chaining Example ===");
    
    // Chain take with other operators
    From(Range(1, 20))
        .Filter([](int x) { return x % 2 == 0; })  // even numbers
        .Take(3)                                    // take first 3
        .Subscribe(
            [](int value) { 
                Serial.print("Chained (even, take 3): "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Chained take completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Chained take error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("Take Operator Examples");
    Serial.println("=====================");
    
    traditional_take_example();
    fluent_take_example();
    take_with_strings_example();
    take_with_map_example();
    take_more_than_available_example();
    take_zero_example();
    take_with_subject_example();
    take_chaining_example();
}

void loop() {
    // Nothing to do in loop
}
