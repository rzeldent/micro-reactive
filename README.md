# Micro-Reactive Library

A comprehensive, lightweight reactive programming library for embedded systems, specifically designed for ESP32 and Arduino platforms using C++11.

## Features

### Core Components

- **IObserver&lt;T&gt;**: Interface for consuming values from observables
- **IObservable&lt;T&gt;**: Interface for sources of values
- **ISubject&lt;T&gt;**: Interface combining both observer and observable
- **Operator&lt;T&gt;**: Base class for reactive operators
- **IDisposable & Subscription**: Resource management with automatic cleanup
- **Thread Safety**: All components use mutexes and atomic operations for thread safety

### Sources (Observable Creators)

- **Create**: Creates an observable from a custom function
- **Range**: Emits a sequence of integers
- **Empty**: Emits no items and completes immediately
- **Never**: Never emits any items
- **Iterate**: Emits each item from a collection
- **Timer**: Emits a single value after a specified delay (non-blocking, threaded)
- **Interval**: Emits sequential numbers at specified intervals (non-blocking, threaded)
- **Defer**: Creates a new observable for each subscriber

### Subjects

- **Subject**: Basic subject that multicasts to multiple observers
- **BehaviorSubject**: Stores the latest value and emits it to new subscribers
- **ReplaySubject**: Replays a specified number of previous values to new subscribers
- **SynchronizedSubject**: Thread-safe subject wrapper

### Transform Operators

- **Map**: Transforms each item using a function
- **Filter**: Filters items based on a predicate
- **Take**: Takes only the first n items
- **Skip**: Skips the first n items
- **Scan**: Applies an accumulator function and emits intermediate results
- **Reduce**: Applies an accumulator function and emits final result
- **Throttle**: Emits only the first item in each time window
- **Buffer**: Buffers items into arrays of specified size
- **Distinct**: Filters out duplicate consecutive values

### Utility Operators

- **First**: Emits only the first value
- **Last**: Emits only the last value
- **Count**: Counts the number of emitted items
- **Sum**: Calculates the sum of all numeric values
- **Min**: Finds the minimum value
- **Max**: Finds the maximum value
- **DefaultIfEmpty**: Provides default value if source is empty
- **StartWith**: Prepends values to the beginning
- **TakeWhile**: Takes items while predicate is true
- **SkipWhile**: Skips items while predicate is true

### Advanced Operators

- **Debounce**: Emits only after a quiet period
- **CombineLatest**: Combines latest values from multiple sources
- **Merge**: Merges multiple observables into one
- **Zip**: Pairs values from multiple sources in sequence
- **Switch**: Switches to latest inner observable (flattens observables of observables)
- **FlatMap/SelectMany**: Flattens inner observables into a single stream
- **Concat**: Concatenates observables sequentially (waits for each to complete)
- **Sample**: Samples the source at specified time intervals
- **WindowTime**: Groups values into time-based windows
- **Delay**: Delays emission of all values by specified duration
- **StartWith**: Starts the sequence with specified initial values

### Error Handling

- **Catch**: Handles errors and provides fallback observables
- **CatchAndReturn**: Simple error handling with fallback values
- **Retry**: Retries on error up to a specified number of times
- **Finally**: Executes cleanup actions on completion or error
- **SafeObserver**: Wraps observers with exception handling

### Schedulers

- **ImmediateScheduler**: Executes work immediately on current thread
- **ThreadPoolScheduler**: Background execution with delayed and periodic scheduling

## Usage Examples

### Basic Observable with New Subscription Pattern

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

// Subscribe and get disposable subscription
auto subscription = range->Subscribe(observer);

// Manually dispose when done (or let it auto-dispose)
subscription->Dispose();
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
auto subscription = subject->Subscribe(observer);

// Emit values
subject->OnNext(10);
subject->OnNext(20);
subject->OnCompleted();
```

### Advanced Operators Chaining

```cpp
// Create a range and apply multiple operators
auto range = Range<int>(1, 10, 1);
auto filtered = Filter<int>(range, [](const int& x) { return x % 2 == 0; });
auto mapped = Map<int, int>(filtered, [](const int& x) { return x * 10; });
auto taken = Take<int>(mapped, 3);

auto subscription = taken->Subscribe(observer);
```

### Error Handling with Retry and Catch

```cpp
// Create an observable that might fail
auto unreliable_source = CreateUnreliableSource();

// Add retry logic
auto with_retry = Retry(unreliable_source, 3);

// Add fallback handling
auto safe_source = CatchAndReturn(with_retry, -1);

auto subscription = safe_source->Subscribe(observer);
```

### Advanced Operators - Debounce and Merge

```cpp
// Debounce rapid fire events
auto debounced = Debounce(user_input_source, std::chrono::milliseconds(300));

// Merge multiple streams
auto stream1 = Timer<int>(1000, 42);
auto stream2 = Interval<int>(500);
auto merged = Merge(stream1, stream2);

auto subscription = merged->Subscribe(observer);
```

### Custom Scheduler Usage

```cpp
// Use background scheduler for heavy work
auto background_scheduler = std::make_shared<ThreadPoolScheduler>();
auto debounced = Debounce(source, std::chrono::milliseconds(100), background_scheduler);

auto subscription = debounced->Subscribe(observer);
```
### BehaviorSubject

```cpp
// Create with initial value
auto behavior = CreateBehaviorSubject<int>(42);

// New subscribers immediately receive the current value
auto subscription = behavior->Subscribe(observer);  // Will immediately receive 42

// Update the value
behavior->OnNext(100);  // All subscribers receive 100
```

### Timer and Interval (Thread-Safe, Non-Blocking)

```cpp
// One-shot timer (fires once after 2 seconds)
auto timer = Timer<unsigned long>(2000);
auto timer_subscription = timer->Subscribe(timerObserver);

// Interval (fires every 1 second)
auto interval = Interval<unsigned long>(1000);
auto interval_subscription = interval->Subscribe(intervalObserver);

// Both run asynchronously and can be disposed at any time
timer_subscription->Dispose();
interval_subscription->Dispose();
```

## Thread Safety and Resource Management

This library provides comprehensive thread safety and automatic resource management:

- **Automatic Cleanup**: All subscriptions are automatically disposed when they go out of scope
- **Thread-Safe Operations**: All observables, operators, and subjects use appropriate synchronization
- **Resource Management**: RAII pattern ensures proper cleanup of threads and resources
- **Memory Safety**: Weak pointers prevent circular dependencies

## Platform Support

- ESP32 (all variants)
- Arduino-compatible boards  
- Requires C++11 compiler support
- Thread support (std::thread, std::mutex, std::atomic)

## Memory Considerations

This library uses smart pointers (std::shared_ptr) for memory management. On resource-constrained devices:

- Use observables judiciously
- Dispose subscriptions when no longer needed
- Consider using shorter chains of operators
- Monitor heap usage with the built-in memory monitoring tools
- Use object pooling for frequently created/destroyed objects

## Building

Use PlatformIO with the provided `platformio.ini` configuration:

```bash
pio build
pio upload
```

## Testing

The library includes comprehensive unit tests using Unity framework:

```bash
pio test
```

## Examples

See `src/main.cpp` for comprehensive examples of all implemented features.

## License

[Your License Here]

## Contributing

This library represents a modern, production-ready reactive programming solution for embedded systems with:

- Comprehensive operator set (25+ operators)
- Advanced error handling and recovery
- Thread safety and resource management
- Extensive test coverage

Perfect for IoT applications, sensor data processing, and real-time embedded systems requiring reactive patterns.
