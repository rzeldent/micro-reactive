#ifndef MICRO_REACTIVE_OPERATORS_FLUENT_H
#define MICRO_REACTIVE_OPERATORS_FLUENT_H

#include "../core.h"
#include "../sources.h"
#include "../subjects.h"
#include "../error_handling.h"
#include "transformation.h"
#include "filtering.h"
#include "basic.h"
#include "aggregation.h"
#include "utility.h"
#include "combination.h"
#include "timing.h"

namespace rx
{
    // =============================================================================
    // FLUENT INTERFACE IMPLEMENTATION
    // =============================================================================

    // Forward declarations
    template <typename T>
    class Observable;

    // Fluent Observable wrapper for method chaining
    template <typename T>
    class Observable
    {
    private:
        std::shared_ptr<IObservable<T>> impl_;

    public:
        // Constructor
        Observable(std::shared_ptr<IObservable<T>> impl) : impl_(impl) {}

        // Get the underlying implementation
        std::shared_ptr<IObservable<T>> Get() const { return impl_; }

        // Delegate subscription
        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer)
        {
            return impl_->Subscribe(observer);
        }

        std::shared_ptr<Subscription> Subscribe(
            std::function<void(const T &)> on_next,
            std::function<void()> on_completed = nullptr,
            std::function<void(const std::exception &)> on_error = nullptr)
        {
            return impl_->Subscribe(CreateObserver<T>(
                std::move(on_next),
                std::move(on_completed),
                std::move(on_error)));
        }

        // Implicit conversion to shared_ptr for compatibility
        operator std::shared_ptr<IObservable<T>>() const { return impl_; }

        // =============================================================================
        // TRANSFORMATION OPERATORS (Fluent)
        // =============================================================================

        template <typename U>
        Observable<U> Map(std::function<U(const T &)> transform) const
        {
            return Observable<U>(rx::Map<T, U>(impl_, transform));
        }

        template <typename TAcc>
        Observable<TAcc> Scan(TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulator) const
        {
            return Observable<TAcc>(rx::Scan<T, TAcc>(impl_, seed, accumulator));
        }

        Observable<double> PID(
            double setpoint, double kp, double ki, double kd, double dt,
            double min_output, double max_output) const
        {
            return Observable<double>(rx::PID<T>(
                impl_, setpoint, kp, ki, kd, dt, min_output, max_output));
        }

        Observable<double> Kalman(
            double process_noise, double measurement_noise,
            double initial_estimate = 0.0,
            double initial_covariance = 1.0) const
        {
            return Observable<double>(rx::Kalman<T>(
                impl_, process_noise, measurement_noise,
                initial_estimate, initial_covariance));
        }

        // =============================================================================
        // FILTERING OPERATORS (Fluent)
        // =============================================================================

        Observable<T> Filter(std::function<bool(const T &)> predicate) const
        {
            return Observable<T>(rx::Filter<T>(impl_, predicate));
        }

        Observable<T> Take(size_t count) const
        {
            return Observable<T>(rx::Take<T>(impl_, count));
        }

        Observable<T> Skip(size_t count) const
        {
            return Observable<T>(rx::Skip<T>(impl_, count));
        }

        Observable<T> TakeWhile(std::function<bool(const T &)> predicate) const
        {
            return Observable<T>(rx::TakeWhile<T>(impl_, predicate));
        }

        Observable<T> SkipWhile(std::function<bool(const T &)> predicate) const
        {
            return Observable<T>(rx::SkipWhile<T>(impl_, predicate));
        }

        Observable<T> First() const
        {
            return Observable<T>(rx::First<T>(impl_));
        }

        Observable<T> Last() const
        {
            return Observable<T>(rx::Last<T>(impl_));
        }

        Observable<T> Distinct() const
        {
            return Observable<T>(rx::Distinct<T>(impl_));
        }

        // =============================================================================
        // AGGREGATION OPERATORS (Fluent)
        // =============================================================================

        template <typename TAcc>
        Observable<TAcc> Reduce(TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulator) const
        {
            return Observable<TAcc>(rx::Reduce<T, TAcc>(impl_, seed, accumulator));
        }

        Observable<size_t> Count() const
        {
            return Observable<size_t>(rx::Count<T>(impl_));
        }

        Observable<T> Sum() const
        {
            return Observable<T>(rx::Sum<T>(impl_));
        }

        Observable<double> Average() const
        {
            return Observable<double>(rx::Average<T>(impl_));
        }

        Observable<T> Min() const
        {
            return Observable<T>(rx::Min<T>(impl_));
        }

        Observable<T> Max() const
        {
            return Observable<T>(rx::Max<T>(impl_));
        }

        Observable<bool> All(std::function<bool(const T &)> predicate) const
        {
            return Observable<bool>(rx::All<T>(impl_, predicate));
        }

        Observable<bool> Any(std::function<bool(const T &)> predicate) const
        {
            return Observable<bool>(rx::Any<T>(impl_, predicate));
        }

        // =============================================================================
        // UTILITY OPERATORS (Fluent)
        // =============================================================================

        Observable<T> Throttle(size_t interval) const
        {
            return Observable<T>(rx::Throttle<T>(impl_, interval));
        }

        Observable<T> Debounce(
            std::chrono::milliseconds duration,
            std::shared_ptr<IScheduler> scheduler =
                detail::BackgroundSchedulerShared()) const
        {
            return Observable<T>(rx::Debounce<T>(
                impl_, duration, scheduler));
        }

        Observable<T> Delay(
            std::chrono::milliseconds duration,
            std::shared_ptr<IScheduler> scheduler =
                detail::BackgroundSchedulerShared()) const
        {
            return Observable<T>(rx::Delay<T>(
                impl_, duration, scheduler));
        }

        Observable<T> Sample(
            std::chrono::milliseconds period,
            std::shared_ptr<IScheduler> scheduler =
                detail::BackgroundSchedulerShared()) const
        {
            return Observable<T>(rx::Sample<T>(
                impl_, period, scheduler));
        }

        Observable<T> Catch(
            std::function<std::shared_ptr<IObservable<T>>(
                const std::exception &)>
                error_handler) const
        {
            return Observable<T>(rx::Catch<T>(impl_, error_handler));
        }

        Observable<T> DefaultIfEmpty(const T &default_value) const
        {
            return Observable<T>(rx::DefaultIfEmpty<T>(impl_, default_value));
        }

        Observable<T> StartWith(const std::vector<T> &start_values) const
        {
            return Observable<T>(rx::StartWith<T>(impl_, start_values));
        }

        Observable<T> Do(std::function<void(const T &)> action) const
        {
            return Observable<T>(rx::Do<T>(impl_, action));
        }

        Observable<T> TakeUntil(std::shared_ptr<IObservable<T>> other) const
        {
            return Observable<T>(rx::TakeUntil<T>(impl_, other));
        }

        Observable<T> SkipUntil(std::shared_ptr<IObservable<T>> other) const
        {
            return Observable<T>(rx::SkipUntil<T>(impl_, other));
        }

        Observable<bool> Contains(const T &value) const
        {
            return Observable<bool>(rx::Contains<T>(impl_, value));
        }

        Observable<T> DistinctUntilChanged() const
        {
            return Observable<T>(rx::DistinctUntilChanged<T>(impl_));
        }

        Observable<std::pair<T, T>> Pairwise() const
        {
            return Observable<std::pair<T, T>>(rx::Pairwise<T>(impl_));
        }

        Observable<T> Debug(const std::string &name) const
        {
            return Observable<T>(rx::Debug<T>(impl_, name));
        }

        // =============================================================================
        // COMBINATION OPERATORS (Fluent)
        // =============================================================================

        Observable<T> Race(std::shared_ptr<IObservable<T>> other) const
        {
            return Observable<T>(rx::Race<T>(impl_, other));
        }

        Observable<T> Merge(std::shared_ptr<IObservable<T>> other) const
        {
            return Observable<T>(rx::Merge<T>(impl_, other));
        }

        Observable<T> Merge(std::shared_ptr<Subject<T>> other) const
        {
            return Merge(std::static_pointer_cast<IObservable<T>>(other));
        }

        Observable<T> Concat(std::shared_ptr<IObservable<T>> other) const
        {
            return Observable<T>(rx::Concat<T>(impl_, other));
        }

        Observable<T> Concat(std::shared_ptr<Subject<T>> other) const
        {
            return Concat(std::static_pointer_cast<IObservable<T>>(other));
        }

        template <typename U>
        Observable<std::pair<T, U>> Zip(
            std::shared_ptr<IObservable<U>> other) const
        {
            return Observable<std::pair<T, U>>(
                rx::Zip<T, U>(impl_, other));
        }

        template <typename U>
        Observable<std::pair<T, U>> Zip(
            std::shared_ptr<Subject<U>> other) const
        {
            return Zip(std::static_pointer_cast<IObservable<U>>(other));
        }

        template <typename U>
        Observable<U> FlatMap(
            std::function<std::shared_ptr<IObservable<U>>(const T &)> mapper)
            const
        {
            return Observable<U>(rx::FlatMap<T, U>(impl_, mapper));
        }

        template <typename U>
        Observable<std::pair<T, U>> WithLatestFrom(
            std::shared_ptr<IObservable<U>> other) const
        {
            return Observable<std::pair<T, U>>(
                rx::WithLatestFrom<T, U>(impl_, other));
        }

        template <typename U>
        Observable<std::pair<T, U>> WithLatestFrom(
            std::shared_ptr<Subject<U>> other) const
        {
            return WithLatestFrom(
                std::static_pointer_cast<IObservable<U>>(other));
        }

        template <typename U>
        Observable<std::pair<T, U>> WithLatestFrom(
            std::shared_ptr<BehaviorSubject<U>> other) const
        {
            return WithLatestFrom(
                std::static_pointer_cast<IObservable<U>>(other));
        }

        template <typename U>
        Observable<U> Switch() const
        {
            return Observable<U>(rx::Switch<U>(impl_));
        }
    };

    // =============================================================================
    // FLUENT HELPER FUNCTIONS
    // =============================================================================

    // Helper function to wrap any IObservable into fluent Observable
    template <typename T>
    Observable<T> From(std::shared_ptr<IObservable<T>> observable)
    {
        return Observable<T>(observable);
    }

    template <typename T>
    Observable<T> From(std::shared_ptr<Subject<T>> subject)
    {
        return Observable<T>(
            std::static_pointer_cast<IObservable<T>>(subject));
    }

    template <typename T>
    Observable<T> From(std::shared_ptr<BehaviorSubject<T>> subject)
    {
        return Observable<T>(
            std::static_pointer_cast<IObservable<T>>(subject));
    }

    // Fluent source creation functions
    template <typename T>
    Observable<T> FluentEmpty()
    {
        return Observable<T>(Empty<T>());
    }

    template <typename T>
    Observable<T> FluentNever()
    {
        return Observable<T>(Never<T>());
    }

    template <typename T>
    Observable<T> FluentRange(T start, T end)
    {
        return Observable<T>(Range<T>(start, end));
    }

    template <typename T>
    Observable<T> FluentFromVector(const std::vector<T> &values)
    {
        return Observable<T>(FromVector<T>(values));
    }

    template <typename T = int>
    Observable<T> FluentTimer(std::chrono::milliseconds delay)
    {
        return Observable<T>(Timer<T>(delay));
    }

    template <typename T = int>
    Observable<T> FluentInterval(std::chrono::milliseconds interval, int count = 5)
    {
        return Observable<T>(Interval<T>(interval, count));
    }

} // namespace rx

#endif // MICRO_REACTIVE_OPERATORS_FLUENT_H
