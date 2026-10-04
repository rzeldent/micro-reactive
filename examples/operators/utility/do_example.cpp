#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * Do Operator Example
 * 
 * Demonstrates the Do operator which performs side effects
 * without modifying the emitted values (like tap in RxJS).
 */

void traditional_do_example() {
    Serial.println("=== Traditional Do Example ===");
    
    // Create a range of numbers
    auto range_observable = Range(1, 5);
    
    // Add side effect to log each value
    auto do_observable = Do(range_observable, 
        [](int value) { 
            Serial.print("Side effect - value: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Side effect - completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Side effect - error: "); 
            Serial.println(e.what()); 
        }
    );
    
    // Subscribe to see the results
    auto observer = CreateObserver<int>(
        [](int value) { 
            Serial.print("Observer received: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Observer completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Observer error: "); 
            Serial.println(e.what()); 
        }
    );
    
    auto subscription = do_observable->Subscribe(observer);
    
    Serial.println();
}

void fluent_do_example() {
    Serial.println("=== Fluent Do Example ===");
    
    // Use fluent interface with side effects (only onNext callback)
    From(Range(1, 5))
        .Do(
            [](int value) { 
                Serial.print("Fluent side effect - value: "); 
                Serial.println(value); 
            }
        )
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent observer received: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent observer completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent observer error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void do_with_map_example() {
    Serial.println("=== Do with Map Example ===");
    
    // Traditional approach - do then map
    auto range_obs = Range(1, 5);
    auto do_obs = Do(range_obs, 
        [](int value) { 
            Serial.print("Before map: "); 
            Serial.println(value); 
        });
    auto mapped_obs = Map<int, int>(do_obs, [](int x) { return x * 2; });
    
    mapped_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional - After map: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional do+map completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional do+map error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach - chain do and map
    From(Range(1, 5))
        .Do(
            [](int value) { 
                Serial.print("Fluent before map: "); 
                Serial.println(value); 
            })
        .Map<int>([](int x) { return x * 2; })
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent after map: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent do+map completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent do+map error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void do_with_filter_example() {
    Serial.println("=== Do with Filter Example ===");
    
    // Traditional approach - filter then do
    auto range_obs = Range(1, 10);
    auto filtered_obs = Filter(range_obs, [](int x) { return x % 2 == 0; });
    auto do_obs = Do(filtered_obs, 
        [](int value) { 
            Serial.print("Filtered even: "); 
            Serial.println(value); 
        });
    
    do_obs->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional - Observer: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional filter+do completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional filter+do error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach - chain filter and do
    From(Range(1, 10))
        .Filter([](int x) { return x % 2 == 0; })
        .Do(
            [](int value) { 
                Serial.print("Fluent filtered even: "); 
                Serial.println(value); 
            })
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent observer: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent filter+do completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent filter+do error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void do_with_strings_example() {
    Serial.println("=== Do with Strings Example ===");
    
    std::vector<std::string> words = {"hello", "world", "from", "do", "operator"};
    
    // Traditional approach
    auto vector_obs = FromVector(words);
    auto do_obs = Do(vector_obs, 
        [](const std::string& value) { 
            Serial.print("Processing: "); 
            Serial.println(value.c_str()); 
        });
    
    do_obs->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            Serial.print("Traditional - Received: "); 
            Serial.println(value.c_str()); 
        },
        []() { 
            Serial.println("Traditional string do completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional string do error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach
    From(FromVector(words))
        .Do(
            [](const std::string& value) { 
                Serial.print("Fluent processing: "); 
                Serial.println(value.c_str()); 
            })
        .Subscribe(
            [](const std::string& value) { 
                Serial.print("Fluent received: "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Fluent string do completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent string do error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void do_error_handling_example() {
    Serial.println("=== Do Error Handling Example ===");
    
    std::vector<int> numbers = {1, 2, 0, 4, 5};
    
    // Do with error handling
    From(FromVector(numbers))
        .Do(
            [](int value) { 
                Serial.print("Before division: "); 
                Serial.println(value); 
            }
        )
        .Map<std::string>([](int x) -> std::string {
            if (x == 0) {
                throw std::runtime_error("Division by zero!");
            }
            return "1/" + std::to_string(x) + " = " + std::to_string(1.0/x);
        })
        .Subscribe(
            [](const std::string& value) { 
                Serial.print("Result: "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Do+map completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Observer caught error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void do_with_subject_example() {
    Serial.println("=== Do with Subject Example ===");
    
    auto subject = CreateSubject<int>();
    
    // Traditional approach
    auto do_subject = Do(subject->AsObservable(), 
        [](int value) { 
            Serial.print("Subject side effect: "); 
            Serial.println(value); 
        });
    
    do_subject->Subscribe(CreateObserver<int>(
        [](int value) { 
            Serial.print("Traditional subject observer: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional subject do completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional subject do error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Fluent approach
    From(subject)
        .Do(
            [](int value) { 
                Serial.print("Fluent subject side effect: "); 
                Serial.println(value); 
            })
        .Subscribe(
            [](int value) { 
                Serial.print("Fluent subject observer: "); 
                Serial.println(value); 
            },
            []() { 
                Serial.println("Fluent subject do completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent subject do error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Push values through subject
    subject->OnNext(10);
    subject->OnNext(20);
    subject->OnNext(30);
    subject->OnCompleted();
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("Do Operator Examples");
    Serial.println("====================");
    
    traditional_do_example();
    fluent_do_example();
    do_with_map_example();
    do_with_filter_example();
    do_with_strings_example();
    do_error_handling_example();
    do_with_subject_example();
}

void loop() {
    // Nothing to do in loop
}
