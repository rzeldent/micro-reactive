# SelectMany Fix Summary

## Issue Fixed
The SelectMany alias for FlatMap was missing a `return` statement, causing compilation errors when using SelectMany.

## Fix Applied
**File:** `include/advanced_operators.h` (line ~910-915)

**Before:**
```cpp
template<typename T, typename R>
std::shared_ptr<FlatMapOperator<T, R>> SelectMany(
    std::shared_ptr<IObservable<T>> source,
    std::function<std::shared_ptr<IObservable<R>>(const T&)> selector) {
    FlatMap<T, R>(source, selector);  // Missing return!
}
```

**After:**
```cpp
template<typename T, typename R>
std::shared_ptr<FlatMapOperator<T, R>> SelectMany(
    std::shared_ptr<IObservable<T>> source,
    std::function<std::shared_ptr<IObservable<R>>(const T&)> selector) {
    return FlatMap<T, R>(source, selector);  // Fixed!
}
```

## Verification
- ✅ Code compiles successfully (`pio run` passes)
- ✅ SelectMany alias now properly returns the FlatMapOperator
- ✅ No compilation errors related to SelectMany

## Status
The SelectMany alias for FlatMap is now working correctly and can be used interchangeably with FlatMap.

## Next Steps
The only remaining issues are the hanging tests (SwitchOperator, ConcatOperator, and Retry with Create observable) which are disabled in the current test suite. These require further debugging to understand why they hang during execution.
