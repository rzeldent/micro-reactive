# Micro-Reactive Examples

The examples are standalone Arduino sketches demonstrating the library's
sources, subjects, operators, schedulers, and fluent API. They are included
in the ESP32 example-build matrix.

## Example files

### Sources

- `sources/range_example.cpp` — emit a sequence of values.
- `sources/fromvector_example.cpp` — emit values from a vector.
- `sources/empty_example.cpp` — complete without emitting values.
- `sources/timer_example.cpp` — emit a value after a delay.
- `sources/interval_example.cpp` — emit values at regular intervals.

### Subjects

- `subjects/subject_example.cpp` — multicast values to subscribers.
- `subjects/behaviorsubject_example.cpp` — retain and replay the latest value.

### Transformation operators

- `operators/transformation/map_example.cpp` — transform each value; includes traditional and fluent usage.
- `operators/transformation/pid_example.cpp` — apply a bounded discrete PID controller to numeric samples; includes traditional and fluent usage.
- `operators/transformation/kalman_example.cpp` — smooth scalar measurements with a Kalman estimate; includes traditional and fluent usage.

### Filtering operators

- `operators/filtering/filter_example.cpp` — emit values matching a predicate.
- `operators/filtering/take_example.cpp` — limit the number of emitted values.

### Aggregation operators

- `operators/aggregation/sum_example.cpp` — sum numeric values.

### Utility operators

- `operators/utility/do_example.cpp` — perform side effects while forwarding
  values.

### Combination operators

- `operators/combination/concat_example.cpp` — subscribe to sources in order.
- `operators/combination/flatmap_example.cpp` — map values to inner sources and forward their values.
- `operators/combination/merge_example.cpp` — forward values from multiple sources.
- `operators/combination/switch_example.cpp` — forward from the latest inner source.
- `operators/combination/with_latest_from_example.cpp` — pair source values with the latest value from another source.
- `operators/combination/zip_example.cpp` — pair values from two sources by position.

### Time-based operators

- `operators/timing/debounce_example.cpp` — emit after a quiet period.
- `operators/timing/delay_example.cpp` — delay values.
- `operators/timing/sample_example.cpp` — periodically emit the latest value.

### Schedulers

- `schedulers/testscheduler_example.cpp` — drive scheduled work deterministically.

### Fluent interface

- `fluent_interface_demo.cpp` — demonstrate subscriptions and the fluent wrapper alongside traditional operator composition.

## Include and run

Examples include the library with:

```cpp
#include <micro-reactive.h>
```

Each example has an Arduino `setup()` and `loop()` entry point. To run one,
select it as the sketch entry point in a PlatformIO project, build for the
target board, upload, and monitor the serial output. The repository CI builds
each `.cpp` file under `examples/` separately for the configured ESP32 boards.
The examples may show traditional factory calls, fluent calls, or both;
consult the individual file for its exact coverage.

Run the host-side unit tests with:

```sh
pio test -e native
```

## PID and Kalman parameters

`PID(source, setpoint, kp, ki, kd, dt, min_output, max_output)` returns
`double` controller outputs. It computes error as `setpoint - sample`, uses
the supplied fixed sample interval `dt`, and clamps each output to the
inclusive output limits. `dt` must be positive; all numeric parameters must
be finite, and `min_output` must not exceed `max_output`.

`Kalman(source, process_noise, measurement_noise, initial_estimate,
initial_covariance)` returns `double` estimates. The initial estimate defaults
to `0.0` and initial covariance to `1.0`. Process noise must be non-negative;
measurement noise must be positive; initial covariance must be non-negative.
Both operators accept arithmetic source sample types and also have fluent
methods, `.PID(...)` and `.Kalman(...)`.

The state belongs to the operator instance, so repeated subscriptions to the
same operator instance share its accumulated filter state.

## Contributing examples

Use the `{operator}_example.cpp` naming convention where applicable, include
an Arduino entry point so the sketch can be built independently, and update
this index when adding an example. Include both traditional and fluent usage
when both APIs are relevant and supported.
