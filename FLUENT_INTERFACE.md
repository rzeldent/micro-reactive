# Fluent Interface for Micro-Reactive

`Observable<T>` wraps a `std::shared_ptr<IObservable<T>>` and supplies the
chainable API implemented in `include/operators/fluent.h`. Use `From()` to
wrap an observable, subject, or behavior subject. `Get()` returns the wrapped
observable; implicit conversion to `std::shared_ptr<IObservable<T>>` is also
available.

## Example

```cpp
auto subscription = From(Range(1, 10))
    .Map<int>([](const int &value) { return value * 2; })
    .Filter([](const int &value) { return value > 5; })
    .Take(3)
    .Subscribe(
        [](const int &value) { Serial.println(value); },
        []() { Serial.println("Completed"); });
```

The traditional factory API remains available and can be mixed with fluent chains:

```cpp
auto fluent = From(Range(1, 10))
    .Map<int>([](const int &value) { return value * 2; });
auto result = Take<int>(fluent.Get(), 3);
```

## Fluent operators

The current wrapper provides the following methods:

- Transformation: `Map`, `Scan`, `PID`, `Kalman`
- Filtering: `Filter`, `Take`, `Skip`, `TakeWhile`, `SkipWhile`, `First`,
  `Last`, `Distinct`
- Aggregation: `Reduce`, `Count`, `Sum`, `Average`, `Min`, `Max`, `All`,
  `Any`
- Utility: `Throttle`, `Debounce`, `Delay`, `Sample`, `Catch`,
  `DefaultIfEmpty`, `StartWith`, `Do`, `TakeUntil`, `SkipUntil`, `Contains`,
  `DistinctUntilChanged`, `Pairwise`, `Debug`
- Combination: `Race`, `Merge`, `Concat`, `Zip`, `FlatMap`,
  `WithLatestFrom`, `Switch`

`Subscribe` accepts either an `IObserver<T>` or callbacks for next, completion,
and error. The callback overload returns a `Subscription`, just like the
traditional API.

## PID and Kalman

Both transforming filters have fluent methods and produce
`Observable<double>`:

```cpp
auto control = From(sensor_values)
    .PID(setpoint, kp, ki, kd, sample_interval, min_output, max_output);

auto smoothed = From(sensor_values)
    .Kalman(process_noise, measurement_noise, initial_estimate,
            initial_covariance);
```

The PID sample interval must be positive and its output is clamped to the
configured limits. Kalman process noise and initial covariance must be
non-negative; measurement noise must be positive. The traditional factory
forms are `PID(source, ...)` and `Kalman(source, ...)`.

## Fluent sources

In addition to `From()`, the header provides `FluentEmpty`,
`FluentNever`, `FluentRange`, `FluentFromVector`, `FluentTimer`, and
`FluentInterval`. These helpers return the same `Observable<T>` wrapper.

See `examples/operators/transformation/` for standalone PID and Kalman
examples using both traditional and fluent forms.
