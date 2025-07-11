# Micro-Reactive Examples

This directory contains comprehensive examples demonstrating all operators, schedulers, sources, and subjects in the micro-reactive library. Each example shows both traditional and fluent interface usage patterns.

## Directory Structure

```
examples/
├── sources/                     # Observable sources
│   ├── range_example.cpp        # Range source - emit sequences of numbers
│   ├── fromvector_example.cpp   # FromVector source - emit from collections
│   ├── empty_example.cpp        # Empty source - completes immediately
│   ├── timer_example.cpp        # Timer source - emit after delay
│   └── interval_example.cpp     # Interval source - emit at intervals
├── subjects/                    # Subject implementations
│   ├── subject_example.cpp      # Basic Subject - multicast observable
│   └── behaviorsubject_example.cpp # BehaviorSubject - stores latest value
├── operators/
│   ├── transformation/         # Value transformation operators
│   │   └── map_example.cpp      # Map operator - transform values
│   ├── filtering/              # Filtering operators
│   │   ├── filter_example.cpp   # Filter operator - conditional emission
│   │   └── take_example.cpp     # Take operator - limit emission count
│   ├── aggregation/            # Aggregation operators
│   │   └── sum_example.cpp      # Sum operator - calculate totals
│   └── utility/                # Utility operators
│       └── do_example.cpp       # Do operator - side effects
└── schedulers/                 # Scheduler implementations
    └── testscheduler_example.cpp # TestScheduler - deterministic testing
```

## Available Examples

### Sources
Sources are the entry points that create observables and emit values.

- **Range** (`range_example.cpp`) - Emits a sequence of integers within a specified range
- **FromVector** (`fromvector_example.cpp`) - Emits all elements from a vector/collection
- **Empty** (`empty_example.cpp`) - Completes immediately without emitting values
- **Timer** (`timer_example.cpp`) - Emits a single value after a specified delay
- **Interval** (`interval_example.cpp`) - Emits values at regular intervals

### Subjects
Subjects are both Observable and Observer - they can emit values and be subscribed to.

- **Subject** (`subject_example.cpp`) - Basic subject for multicasting
- **BehaviorSubject** (`behaviorsubject_example.cpp`) - Stores and immediately emits the latest value to new subscribers

### Operators

#### Transformation Operators
Transform emitted values into new forms.

- **Map** (`transformation/map_example.cpp`) - Transform each value using a function

#### Filtering Operators
Control which values are emitted based on conditions.

- **Filter** (`filtering/filter_example.cpp`) - Emit only values that satisfy a predicate
- **Take** (`filtering/take_example.cpp`) - Emit only the first N values

#### Aggregation Operators
Combine multiple values into single results.

- **Sum** (`aggregation/sum_example.cpp`) - Calculate the sum of all emitted numeric values

#### Utility Operators
Provide additional functionality without changing the core data flow.

- **Do** (`utility/do_example.cpp`) - Perform side effects without modifying values

### Schedulers
Control the execution context and timing of operations.

- **TestScheduler** (`schedulers/testscheduler_example.cpp`) - Deterministic scheduler for testing time-based operations

## Example Patterns

Each example demonstrates two main usage patterns:

### Traditional Pattern
```cpp
// Create observables
auto range_obs = Range(1, 5);
auto filtered_obs = Filter(range_obs, [](int x) { return x % 2 == 0; });
auto mapped_obs = Map<int, string>(filtered_obs, [](int x) { return to_string(x); });

// Subscribe
mapped_obs->Subscribe(CreateObserver<string>(
    [](const string& value) { cout << value << endl; },
    []() { cout << "Completed" << endl; }
));
```

### Fluent Pattern
```cpp
// Chain operations fluently
Observable(Range(1, 5))
    .Filter([](int x) { return x % 2 == 0; })
    .Map<string>([](int x) { return to_string(x); })
    .Subscribe(
        [](const string& value) { cout << value << endl; },
        []() { cout << "Completed" << endl; }
    );
```

## Running Examples

Each example is a standalone Arduino/PlatformIO sketch that can be compiled and run:

1. Copy the desired example to your main sketch file
2. Include the micro-reactive library: `#include "micro-reactive.h"`
3. Compile and upload to your target device
4. Monitor serial output to see the results

## Example Features Demonstrated

### Basic Concepts
- Creating observables from various sources
- Subscribing with observers
- Handling completion and errors
- Memory management with smart pointers

### Advanced Patterns
- Operator chaining and composition
- Subject multicasting
- Side effects and debugging
- Time-based operations
- Error handling and recovery
- Performance considerations

### Real-world Scenarios
- Data processing pipelines
- Event handling
- State management
- Asynchronous operations
- Testing reactive code

## ESP32 Compatibility

All examples are designed to work on embedded systems, particularly ESP32:

- Memory-efficient implementations
- No dynamic allocation in hot paths
- Thread-safe operations where needed
- Reasonable resource usage

## Additional Resources

- See the main README.md for library overview
- Check test files for comprehensive operator coverage
- Review include/ directory for full API documentation
- Explore src/ directory for implementation details

## Contributing Examples

When adding new examples:

1. Follow the established naming convention: `{operator_name}_example.cpp`
2. Include both traditional and fluent usage patterns
3. Demonstrate practical use cases
4. Add error handling examples where relevant
5. Update this README with the new example
6. Ensure ESP32 compatibility

Each example should be self-contained and educational, showing not just how to use the operator but why and when you might want to use it.
