# Enhanced Operators Documentation

This document describes all the additional operators that have been added to the micro-reactive library, expanding its functionality beyond the original set.

## Original Operators (Previously Available)
- **Map** - Transforms each emitted item using a function
- **Filter** - Filters items based on a predicate
- **Take** - Takes the first N items
- **Skip** - Skips the first N items
- **Distinct** - Removes duplicate values
- **Scan** - Running accumulation (emits intermediate results)
- **Reduce** - Final accumulation (emits only final result)
- **First** - Emits only the first item
- **Last** - Emits only the last item
- **Throttle** - Emits every Nth item
- **Where** - Alias for Filter (LINQ-style)
- **Select** - Alias for Map (LINQ-style)

## New Operators Added

### 1. Buffer
**Purpose**: Groups emitted items into buffers of a specified size.

**Signature**: `Buffer<T>(observable, bufferSize)`

**Example**:
```cpp
auto source = rx::Range(1, 7);
auto buffered = rx::Buffer(source, 3);
// Emits: [1,2,3], [4,5,6], [7]
```

**Use Cases**:
- Batching data for processing
- Creating fixed-size chunks for transmission
- Buffering sensor readings

### 2. TakeWhile
**Purpose**: Takes items while a condition is true.

**Signature**: `TakeWhile<T>(observable, predicate)`

**Example**:
```cpp
auto source = rx::Range(1, 10);
auto takeWhile = rx::TakeWhile(source, [](const int& x) { return x < 5; });
// Emits: 1, 2, 3, 4
```

**Use Cases**:
- Taking values until a condition is met
- Processing data until a threshold
- Conditional data collection

### 3. SkipWhile
**Purpose**: Skips items while a condition is true, then emits all remaining items.

**Signature**: `SkipWhile<T>(observable, predicate)`

**Example**:
```cpp
auto source = rx::Range(1, 8);
auto skipWhile = rx::SkipWhile(source, [](const int& x) { return x < 5; });
// Emits: 5, 6, 7, 8
```

**Use Cases**:
- Skipping initial unwanted values
- Ignoring startup transients
- Waiting for a condition to be met

### 4. StartWith
**Purpose**: Prepends values to the beginning of the sequence.

**Signature**: 
- `StartWith<T>(observable, std::vector<T> startValues)`
- `StartWith<T>(observable, T startValue)` (single value overload)

**Example**:
```cpp
auto source = rx::Range(5, 3);
auto startWith = rx::StartWith(source, {1, 2, 3});
// Emits: 1, 2, 3, 5, 6, 7
```

**Use Cases**:
- Adding header values
- Initializing sequences
- Providing default startup values

### 5. DefaultIfEmpty
**Purpose**: Emits a default value if the sequence is empty.

**Signature**: `DefaultIfEmpty<T>(observable, defaultValue)`

**Example**:
```cpp
auto emptySource = rx::FromVector<int>({});
auto defaultIfEmpty = rx::DefaultIfEmpty(emptySource, 42);
// Emits: 42 (because source is empty)

auto nonEmptySource = rx::Range(1, 2);
auto defaultIfEmpty2 = rx::DefaultIfEmpty(nonEmptySource, 42);
// Emits: 1, 2 (original values, no default)
```

**Use Cases**:
- Handling empty sequences gracefully
- Providing fallback values
- Ensuring at least one value is emitted

### 6. Count
**Purpose**: Counts the number of items emitted and returns the count.

**Signature**: `Count<T>(observable)` (returns `size_t`)

**Example**:
```cpp
auto source = rx::Range(1, 5);
auto count = rx::Count(source);
// Emits: 5 (count of items)
```

**Use Cases**:
- Getting sequence length
- Statistics collection
- Validation of data sets

### 7. Sum
**Purpose**: Calculates the sum of numeric items.

**Signature**: `Sum<T>(observable)` (T must support `+=` operator)

**Example**:
```cpp
auto source = rx::Range(1, 5);
auto sum = rx::Sum(source);
// Emits: 15 (1+2+3+4+5)
```

**Use Cases**:
- Mathematical calculations
- Totaling sensor readings
- Aggregating numeric data

### 8. Min
**Purpose**: Finds the minimum value in the sequence.

**Signature**: `Min<T>(observable)` (T must support `<` operator)

**Example**:
```cpp
auto source = rx::FromVector<int>({5, 2, 8, 1, 9});
auto min = rx::Min(source);
// Emits: 1 (minimum value)
```

**Use Cases**:
- Finding minimum sensor reading
- Statistical analysis
- Range validation

### 9. Max
**Purpose**: Finds the maximum value in the sequence.

**Signature**: `Max<T>(observable)` (T must support `>` operator)

**Example**:
```cpp
auto source = rx::FromVector<int>({5, 2, 8, 1, 9});
auto max = rx::Max(source);
// Emits: 9 (maximum value)
```

**Use Cases**:
- Finding peak values
- Statistical analysis
- Range validation

## New Source: FromVector
A new source has also been added to create observables from vectors:

**Signature**: `FromVector<T>(const std::vector<T>& values)`

**Example**:
```cpp
auto source = rx::FromVector<int>({1, 2, 3, 4, 5});
// Emits: 1, 2, 3, 4, 5
```

## Enhanced Range Function
The Range function now has an overload that accepts just two parameters:

**Signatures**:
- `Range<T>(first, last, step)` (original)
- `Range<T>(first, count)` (new - uses step of 1)

**Example**:
```cpp
auto range1 = rx::Range(1, 5);     // Emits: 1, 2, 3, 4, 5 (count of 5)
auto range2 = rx::Range(1, 5, 1);  // Emits: 1, 2, 3, 4, 5 (first to last)
```

## Operator Chaining Examples

All operators can be chained together for complex data processing:

```cpp
auto source = rx::Range(1, 20);
auto result = rx::TakeWhile<int>(
    rx::StartWith<int>(
        rx::Filter(source, [](const int& x) { return x % 2 == 0; }),
        0
    ),
    [](const int& x) { return x < 15; }
);
// Starts with 0, filters even numbers from 1-20, takes while < 15
// Emits: 0, 2, 4, 6, 8, 10, 12, 14
```

## Memory Considerations

All operators are designed for C++11 compatibility and use `std::shared_ptr` for memory management. They are lightweight and suitable for embedded systems like ESP32/Arduino.

## Performance Notes

- **Buffer**: Uses `std::vector` internally, consider buffer size for memory constraints
- **Aggregation operators** (Sum, Min, Max, Count): Process entire sequence before emitting
- **Conditional operators** (TakeWhile, SkipWhile): Stop processing when condition changes
- **StartWith**: Emits prepended values synchronously during subscription

## Error Handling

All operators properly propagate errors and completion signals through the reactive chain. If an error occurs in any part of the chain, it will be propagated to all subscribed observers.
