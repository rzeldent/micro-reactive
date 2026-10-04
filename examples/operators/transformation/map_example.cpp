#include <micro-reactive.h>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * Map Operator Example
 * 
 * Demonstrates the Map operator which transforms each emitted value
 * using a provided function.
 */

void traditional_map_example() {
    Serial.println("=== Traditional Map Example ===");
    
    // Create a range of numbers
    auto range_observable = Range(1, 5);
    
    // Map each number to its square
    auto mapped_observable = Map<int, int>(range_observable, [](int x) {
        return x * x;
    });
    
    // Subscribe to see the results
    auto observer = CreateObserver<int>(
        [](int value) { 
            Serial.print("Squared value: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Map operation completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Error: "); 
            Serial.println(e.what()); 
        }
    );
    
    auto subscription = mapped_observable->Subscribe(observer);
    
    Serial.println();
}

void fluent_map_example() {
    Serial.println("=== Fluent Map Example ===");
    
    // Use fluent interface to map numbers to strings
    From(Range(1, 5))
        .Map<std::string>([](int x) {
            return "Number: " + std::to_string(x);
        })
        .Subscribe(
            [](const std::string& value) { 
                Serial.print("Mapped string: "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Fluent map completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void map_type_conversion_example() {
    Serial.println("=== Map Type Conversion Example ===");
    
    std::vector<int> numbers = {10, 20, 30, 40, 50};
    
    // Traditional approach - convert int to double and scale
    auto vector_obs = FromVector(numbers);
    auto scaled_obs = Map<int, double>(vector_obs, [](int x) {
        return x / 10.0;
    });
    
    scaled_obs->Subscribe(CreateObserver<double>(
        [](double value) { 
            Serial.print("Traditional - Scaled: "); 
            Serial.println(value); 
        },
        []() { 
            Serial.println("Traditional scaling completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional scaling error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
    // Fluent approach - convert to percentage strings
    From(FromVector(numbers))
        .Map<std::string>([](int x) {
            return std::to_string(x) + "%";
        })
        .Subscribe(
            [](const std::string& value) { 
                Serial.print("Fluent - Percentage: "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Fluent percentage conversion completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent percentage conversion error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void map_chaining_example() {
    Serial.println("=== Map Chaining Example ===");
    
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
            Serial.print("Traditional chain: "); 
            Serial.println(value.c_str()); 
        },
        []() { 
            Serial.println("Traditional chain completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional chain error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    Serial.println();
    
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
                Serial.print("Fluent chain: "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Fluent chain completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent chain error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void map_with_subject_example() {
    Serial.println("=== Map with Subject Example ===");
    
    auto subject = CreateSubject<int>();
    
    // Traditional approach
    auto mapped_subject = Map<int, std::string>(subject->AsObservable(), [](int x) {
        if (x < 0) return "Negative: " + std::to_string(x);
        else if (x == 0) return std::string("Zero");
        else return "Positive: " + std::to_string(x);
    });
    
    mapped_subject->Subscribe(CreateObserver<std::string>(
        [](const std::string& value) { 
            Serial.print("Traditional subject map: "); 
            Serial.println(value.c_str()); 
        },
        []() { 
            Serial.println("Traditional subject map completed"); 
        },
        [](const std::exception& e) { 
            Serial.print("Traditional subject map error: "); 
            Serial.println(e.what()); 
        }
    ));
    
    // Fluent approach
    From(subject)
        .Map<std::string>([](int x) {
            return "Absolute: " + std::to_string(std::abs(x));
        })
        .Subscribe(
            [](const std::string& value) { 
                Serial.print("Fluent subject map: "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Fluent subject map completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Fluent subject map error: "); 
                Serial.println(e.what()); 
            }
        );
    
    // Push values through subject
    subject->OnNext(-5);
    subject->OnNext(0);
    subject->OnNext(10);
    subject->OnNext(-3);
    
    subject->OnCompleted();
    
    Serial.println();
}

void map_complex_transformation_example() {
    Serial.println("=== Map Complex Transformation Example ===");
    
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
                Serial.println(result.c_str());
            },
            []() { 
                Serial.println("Complex transformation completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Complex transformation error: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void map_error_handling_example() {
    Serial.println("=== Map Error Handling Example ===");
    
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
                Serial.print("Result: "); 
                Serial.println(value.c_str()); 
            },
            []() { 
                Serial.println("Map with error handling completed"); 
            },
            [](const std::exception& e) { 
                Serial.print("Caught error in map: "); 
                Serial.println(e.what()); 
            }
        );
    
    Serial.println();
}

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("Map Operator Examples");
    Serial.println("====================");
    
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
