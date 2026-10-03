# Operators

The table lists the implemented observable operators and error-handling
operators, grouped by purpose. Each operator has traditional factory-function
usage; the fluent interface is available for operators that expose a matching
method.

## Filtering

| Operator | Purpose | Implementation |
| --- | --- | --- |
| `Filter` | Emits values that satisfy a predicate. | `include/operators/filtering.h` |
| `Take` | Emits at most the first N values. | `include/operators/filtering.h` |
| `Skip` | Skips the first N values. | `include/operators/filtering.h` |
| `TakeWhile` | Emits values while a predicate remains true. | `include/operators/filtering.h` |
| `SkipWhile` | Skips values while a predicate remains true. | `include/operators/filtering.h` |
| `First` | Emits the first source value. | `include/operators/filtering.h` |
| `Last` | Emits the last source value on completion. | `include/operators/filtering.h` |
| `Distinct` | Filters duplicate values. | `include/operators/filtering.h` |

## Transformation

| Operator | Purpose | Implementation |
| --- | --- | --- |
| `Map` | Transforms each value using a mapping function. | `include/operators/transformation.h` |
| `Scan` | Emits each intermediate accumulator result. | `include/operators/transformation.h` |

## Aggregation

| Operator | Purpose | Implementation |
| --- | --- | --- |
| `Reduce` | Emits one accumulated result when the source completes. | `include/operators/aggregation.h` |
| `All` | Emits whether every value satisfies a predicate. | `include/operators/aggregation.h` |
| `Any` | Emits whether any value satisfies a predicate. | `include/operators/aggregation.h` |
| `Count` | Emits the number of source values on completion. | `include/operators/aggregation.h` |
| `Sum` | Emits the sum of numeric source values on completion. | `include/operators/aggregation.h` |
| `Average` | Emits the arithmetic mean on completion. | `include/operators/aggregation.h` |
| `Min` | Emits the smallest source value on completion. | `include/operators/aggregation.h` |
| `Max` | Emits the largest source value on completion. | `include/operators/aggregation.h` |

## Utility

| Operator | Purpose | Implementation |
| --- | --- | --- |
| `DefaultIfEmpty` | Emits a fallback if the source completes without values. | `include/operators/basic.h` |
| `StartWith` | Emits initial values before source values. | `include/operators/basic.h` |
| `Do` | Runs a side effect for each value without changing the stream. | `include/operators/utility.h` |
| `Contains` | Emits whether the source contains a specified value. | `include/operators/utility.h` |
| `Throttle` | Emits every Nth source value. | `include/operators/utility.h` |
| `TakeUntil` | Emits source values until a trigger emits or completes. | `include/operators/utility.h` |
| `SkipUntil` | Skips source values until a trigger emits or completes. | `include/operators/utility.h` |
| `DistinctUntilChanged` | Filters consecutive equal values. | `include/operators/utility.h` |
| `Pairwise` | Emits adjacent source values as pairs. | `include/operators/utility.h` |
| `Debug` | Passes values through while recording observable metrics. | `include/operators/utility.h` |

## Combination

| Operator | Purpose | Implementation |
| --- | --- | --- |
| `Merge` | Forwards values from multiple sources and completes when all complete. | `include/operators/combination.h` |
| `Zip` | Pairs corresponding values from two sources. | `include/operators/combination.h` |
| `FlatMap` | Maps values to inner sources and merges their emissions. | `include/operators/combination.h` |
| `Concat` | Subscribes to sources sequentially in order. | `include/operators/combination.h` |
| `Switch` | Forwards values only from the latest inner source. | `include/operators/combination.h` |
| `WithLatestFrom` | Pairs source values with the latest secondary value. | `include/operators/combination.h` |
| `Race` | Forwards values from the first source to emit. | `include/operators/combination.h` |

## Timing

| Operator | Purpose | Implementation |
| --- | --- | --- |
| `Debounce` | Emits the latest value after a quiet interval. | `include/operators/timing.h` |
| `Delay` | Delays each value by the specified interval. | `include/operators/timing.h` |
| `Sample` | Emits the latest value at regular intervals. | `include/operators/timing.h` |

Time-based operators accept an `IScheduler`; by default they use the
background scheduler. Pass a `TestScheduler` to test timing deterministically.

## Error handling

| Operator | Purpose | Implementation |
| --- | --- | --- |
| `Catch` | Replaces an errored source with a fallback observable. | `include/error_handling.h` |
| `CatchAndReturn` | Replaces an error with a fallback value. | `include/error_handling.h` |
| `Retry` | Resubscribes to the source up to a retry limit. | `include/error_handling.h` |
| `Finally` | Runs cleanup when the source completes or errors. | `include/error_handling.h` |
| `OnErrorResumeNext` | Switches to a fallback source after an error. | `include/error_handling.h` |
| `TimeoutError` | Reports an error if the source does not emit in time. | `include/error_handling.h` |
