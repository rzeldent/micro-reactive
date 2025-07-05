# Micro-Reactive Library

A lightweight reactive programming library for embedded systems, specifically designed for ESP32 and Arduino platforms using C++11.

## Features

### Core Components

- **IObserver<T>**: Interface for consuming values from observables
- **IObservable<T>**: Interface for sources of values
- **ISubject<T>**: Interface combining both observer and observable
- **Operator<T>**: Base class for reactive operators

### Sources (Observable Creators)

- **Create**: Creates an observable from a custom function
- **Range**: Emits a sequence of integers
- **Empty**: Emits no items and completes immediately
- **Never**: Never emits any items
- **Iterate**: Emits each item from a collection
- **Timer**: Emits a single value after a specified delay
- **Interval**: Emits sequential numbers at specified intervals
- **Defer**: Creates a new observable for each subscriber

### Subjects

- **Subject**: Basic subject that multicasts to multiple observers
- **BehaviorSubject**: Stores the latest value and emits it to new subscribers
- **ReplaySubject**: Replays a specified number of previous values to new subscribers
- **SynchronizedSubject**: Thread-safe subject wrapper

### Operators

#### Transform Operators
- **Map**: Transforms each item using a function
- **Filter**: Filters items based on a predicate
- **BufferCount**: Buffers items into arrays of specified size
- **Take**: Takes only the first n items
- **Skip**: Skips the first n items

#### Conditional Operators
- **All**: Tests if all items satisfy a predicate
- **Any**: Tests if any item satisfies a predicate
- **Amb**: Returns the first observable to emit

#### Combine Operators
- **CombineLatest**: Combines latest values from multiple observables
- **Concat**: Concatenates multiple observables sequentially

## Usage Examples

### Basic Observable

```cpp
#include "micro-reactive.h"
using namespace rx;

// Create a range observable
auto range = Range<int>(1, 5, 1);

// Create an observer
auto observer = CreateObserver<int>(
    [](const int& value) { 
        Serial.print("Value: "); 
        Serial.println(value); 
    },
    []() { 
        Serial.println("Completed"); 
    }
);

// Subscribe
range->Subscribe(observer);
```

### Subject Example

```cpp
// Create a subject
auto subject = CreateSubject<int>();

// Subscribe to it
auto observer = CreateObserver<int>(
    [](const int& value) { 
        Serial.println(value); 
    }
);
subject->Subscribe(observer);

// Emit values
subject->OnNext(10);
subject->OnNext(20);
subject->OnCompleted();
```

### Operators Chaining

```cpp
// Create a range and apply operators
auto range = Range<int>(1, 10, 1);
auto filtered = Filter<int>(range, [](const int& x) { return x % 2 == 0; });
auto mapped = Map<int, int>(filtered, [](const int& x) { return x * 10; });
auto taken = Take<int>(mapped, 3);

taken->Subscribe(observer);
```

### BehaviorSubject

```cpp
// Create with initial value
auto behavior = CreateBehaviorSubject<int>(42);

// New subscribers immediately receive the current value
behavior->Subscribe(observer);  // Will immediately receive 42

// Update the value
behavior->OnNext(100);  // All subscribers receive 100
```

### Timer and Interval

```cpp
// One-shot timer (fires once after 2 seconds)
auto timer = Timer<unsigned long>(2000);
timer->Subscribe(timerObserver);

// Interval (fires every 1 second)
auto interval = Interval<unsigned long>(1000);
interval->Subscribe(intervalObserver);
```

## Platform Support

- ESP32 (all variants)
- Arduino-compatible boards
- Requires C++11 compiler support

## Memory Considerations

This library uses smart pointers (std::shared_ptr) for memory management. On resource-constrained devices:

- Use observables judiciously
- Unsubscribe when no longer needed
- Consider using shorter chains of operators
- Monitor heap usage in complex scenarios

## Thread Safety

- **SynchronizedSubject**: Thread-safe subject implementation
- Other components are not inherently thread-safe
- Use appropriate synchronization for multi-threaded scenarios

## Building

Use PlatformIO with the provided `platformio.ini` configuration:

```bash
pio build
pio upload
```

## Examples

See `src/main.cpp` for comprehensive examples of all implemented features.

## License

[Your License Here]

## Reactive extensions for micro controllers
