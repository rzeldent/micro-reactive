#pragma once

#include <core.h>
#include <scheduler.h>
#include <subjects.h>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace rx
{
    namespace detail
    {
        inline std::shared_ptr<IScheduler> BackgroundSchedulerShared()
        {
            return std::shared_ptr<IScheduler>(
                &Schedulers::Background(), [](IScheduler *) {});
        }
    }

    // Debounce emits the newest value after the source has been quiet.
    template <typename T>
    class DebounceObservable : public IObservable<T>
    {
        class State : public std::enable_shared_from_this<State>
        {
            std::shared_ptr<IObserver<T>> downstream_;
            std::shared_ptr<IScheduler> scheduler_;
            std::chrono::milliseconds duration_;
            std::mutex mutex_;
            std::shared_ptr<IScheduledWork> work_;
            std::shared_ptr<T> latest_;
            std::size_t generation_ = 0;
            bool has_value_ = false;
            bool source_completed_ = false;
            bool stopped_ = false;

        public:
            State(std::shared_ptr<IObserver<T>> downstream,
                  std::chrono::milliseconds duration,
                  std::shared_ptr<IScheduler> scheduler)
                : downstream_(downstream), scheduler_(scheduler),
                  duration_(duration)
            {
            }

            void Next(const T &value)
            {
                std::shared_ptr<IScheduledWork> previous;
                std::size_t generation;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    generation = ++generation_;
                    latest_ = std::make_shared<T>(value);
                    has_value_ = true;
                    previous = work_;
                    work_.reset();
                }
                if (previous)
                    previous->Cancel();

                std::weak_ptr<State> weak = this->shared_from_this();
                auto work = scheduler_->ScheduleDelayed(
                    [weak, generation]() {
                        if (auto self = weak.lock())
                            self->Flush(generation);
                    },
                    duration_);
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (!stopped_ && generation == generation_ && has_value_)
                        work_ = work;
                    else
                        work->Cancel();
                }
            }

            void Complete()
            {
                bool complete_now = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    source_completed_ = true;
                    if (!has_value_)
                    {
                        stopped_ = true;
                        complete_now = true;
                    }
                }
                if (complete_now)
                    downstream_->OnCompleted();
            }

            void Error(const std::exception &error)
            {
                std::shared_ptr<IScheduledWork> work;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                    work = work_;
                    work_.reset();
                }
                if (work)
                    work->Cancel();
                downstream_->OnError(error);
            }

            void Dispose()
            {
                std::shared_ptr<IScheduledWork> work;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    stopped_ = true;
                    work = work_;
                    work_.reset();
                }
                if (work)
                    work->Cancel();
            }

        private:
            void Flush(std::size_t generation)
            {
                std::shared_ptr<IObserver<T>> downstream;
                std::shared_ptr<IScheduledWork> work;
                std::shared_ptr<T> value;
                bool emit = false;
                bool complete = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_ || generation != generation_ || !has_value_)
                        return;
                    value = latest_;
                    has_value_ = false;
                    work = work_;
                    work_.reset();
                    downstream = downstream_;
                    emit = true;
                    if (source_completed_)
                    {
                        stopped_ = true;
                        complete = true;
                    }
                }
                if (work)
                    work->Cancel();
                if (emit)
                    downstream->OnNext(*value);
                if (complete)
                    downstream->OnCompleted();
            }
        };

        std::shared_ptr<IObservable<T>> source_;
        std::chrono::milliseconds duration_;
        std::shared_ptr<IScheduler> scheduler_;

    public:
        DebounceObservable(std::shared_ptr<IObservable<T>> source,
                           std::chrono::milliseconds duration,
                           std::shared_ptr<IScheduler> scheduler)
            : source_(source), duration_(duration), scheduler_(scheduler)
        {
        }

        std::shared_ptr<Subscription> Subscribe(
            std::shared_ptr<IObserver<T>> observer) override
        {
            auto state = std::make_shared<State>(
                observer, duration_, scheduler_);
            auto source = source_->Subscribe(CreateObserver<T>(
                [state](const T &value) { state->Next(value); },
                [state]() { state->Complete(); },
                [state](const std::exception &error) { state->Error(error); }));
            return std::make_shared<Subscription>(
                [state, source]() {
                    state->Dispose();
                    source->Dispose();
                });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>>) override {}
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Debounce(
        std::shared_ptr<IObservable<T>> source,
        std::chrono::milliseconds duration,
        std::shared_ptr<IScheduler> scheduler =
            detail::BackgroundSchedulerShared())
    {
        if (!source)
            throw std::invalid_argument(
                "Debounce requires a valid source");
        if (duration.count() < 0)
            throw std::invalid_argument("Debounce duration cannot be negative");
        if (!scheduler)
            throw std::invalid_argument("Debounce requires a scheduler");
        return std::make_shared<DebounceObservable<T>>(
            source, duration, scheduler);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Debounce(
        std::shared_ptr<Subject<T>> source,
        std::chrono::milliseconds duration,
        std::shared_ptr<IScheduler> scheduler =
            detail::BackgroundSchedulerShared())
    {
        return Debounce<T>(
            std::static_pointer_cast<IObservable<T>>(source),
            duration, scheduler);
    }

    // Delay defers each item by the given duration and completes after all
    // delayed items have been delivered.
    template <typename T>
    class DelayObservable : public IObservable<T>
    {
        class State : public std::enable_shared_from_this<State>
        {
            std::shared_ptr<IObserver<T>> downstream_;
            std::shared_ptr<IScheduler> scheduler_;
            std::chrono::milliseconds duration_;
            std::mutex mutex_;
            std::vector<std::shared_ptr<IScheduledWork>> work_;
            std::size_t pending_ = 0;
            bool source_completed_ = false;
            bool stopped_ = false;

        public:
            State(std::shared_ptr<IObserver<T>> downstream,
                  std::chrono::milliseconds duration,
                  std::shared_ptr<IScheduler> scheduler)
                : downstream_(downstream), scheduler_(scheduler),
                  duration_(duration)
            {
            }

            void Next(const T &value)
            {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    ++pending_;
                }
                std::weak_ptr<State> weak = this->shared_from_this();
                auto work = scheduler_->ScheduleDelayed(
                    [weak, value]() {
                        if (auto self = weak.lock())
                            self->Emit(value);
                    },
                    duration_);
                std::lock_guard<std::mutex> lock(mutex_);
                if (stopped_)
                    work->Cancel();
                else
                    work_.push_back(work);
            }

            void Complete()
            {
                bool complete = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    source_completed_ = true;
                    if (pending_ == 0)
                    {
                        stopped_ = true;
                        complete = true;
                    }
                }
                if (complete)
                    downstream_->OnCompleted();
            }

            void Error(const std::exception &error)
            {
                std::vector<std::shared_ptr<IScheduledWork>> work;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                    work.swap(work_);
                }
                for (const auto &item : work)
                    item->Cancel();
                downstream_->OnError(error);
            }

            void Dispose()
            {
                std::vector<std::shared_ptr<IScheduledWork>> work;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    stopped_ = true;
                    work.swap(work_);
                }
                for (const auto &item : work)
                    item->Cancel();
            }

        private:
            void Emit(const T &value)
            {
                bool complete = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    if (pending_ > 0)
                        --pending_;
                    if (source_completed_ && pending_ == 0)
                    {
                        stopped_ = true;
                        complete = true;
                    }
                }
                downstream_->OnNext(value);
                if (complete)
                    downstream_->OnCompleted();
            }
        };

        std::shared_ptr<IObservable<T>> source_;
        std::chrono::milliseconds duration_;
        std::shared_ptr<IScheduler> scheduler_;

    public:
        DelayObservable(std::shared_ptr<IObservable<T>> source,
                        std::chrono::milliseconds duration,
                        std::shared_ptr<IScheduler> scheduler)
            : source_(source), duration_(duration), scheduler_(scheduler)
        {
        }

        std::shared_ptr<Subscription> Subscribe(
            std::shared_ptr<IObserver<T>> observer) override
        {
            auto state = std::make_shared<State>(
                observer, duration_, scheduler_);
            auto source = source_->Subscribe(CreateObserver<T>(
                [state](const T &value) { state->Next(value); },
                [state]() { state->Complete(); },
                [state](const std::exception &error) { state->Error(error); }));
            return std::make_shared<Subscription>(
                [state, source]() {
                    state->Dispose();
                    source->Dispose();
                });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>>) override {}
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Delay(
        std::shared_ptr<IObservable<T>> source,
        std::chrono::milliseconds duration,
        std::shared_ptr<IScheduler> scheduler =
            detail::BackgroundSchedulerShared())
    {
        if (!source)
            throw std::invalid_argument("Delay requires a valid source");
        if (duration.count() < 0)
            throw std::invalid_argument("Delay duration cannot be negative");
        if (!scheduler)
            throw std::invalid_argument("Delay requires a scheduler");
        return std::make_shared<DelayObservable<T>>(
            source, duration, scheduler);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Delay(
        std::shared_ptr<Subject<T>> source,
        std::chrono::milliseconds duration,
        std::shared_ptr<IScheduler> scheduler =
            detail::BackgroundSchedulerShared())
    {
        return Delay<T>(
            std::static_pointer_cast<IObservable<T>>(source),
            duration, scheduler);
    }

    // Sample emits the latest available value at regular intervals.
    template <typename T>
    class SampleObservable : public IObservable<T>
    {
        class State : public std::enable_shared_from_this<State>
        {
            std::shared_ptr<IObserver<T>> downstream_;
            std::mutex mutex_;
            std::shared_ptr<IScheduledWork> work_;
            std::shared_ptr<T> latest_;
            bool has_value_ = false;
            bool stopped_ = false;

        public:
            explicit State(std::shared_ptr<IObserver<T>> downstream)
                : downstream_(downstream)
            {
            }

            void SetWork(std::shared_ptr<IScheduledWork> work)
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (stopped_)
                    work->Cancel();
                else
                    work_ = work;
            }

            void Next(const T &value)
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (!stopped_)
                {
                    latest_ = std::make_shared<T>(value);
                    has_value_ = true;
                }
            }

            void SampleLatest()
            {
                std::shared_ptr<IObserver<T>> downstream;
                std::shared_ptr<T> value;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_ || !has_value_)
                        return;
                    value = latest_;
                    has_value_ = false;
                    downstream = downstream_;
                }
                downstream->OnNext(*value);
            }

            void Complete()
            {
                std::shared_ptr<IScheduledWork> work;
                std::shared_ptr<T> value;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                    work = work_;
                    work_.reset();
                    if (has_value_)
                    {
                        value = latest_;
                        has_value_ = false;
                    }
                }
                if (work)
                    work->Cancel();
                if (value)
                    downstream_->OnNext(*value);
                downstream_->OnCompleted();
            }

            void Error(const std::exception &error)
            {
                std::shared_ptr<IScheduledWork> work;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                    work = work_;
                    work_.reset();
                }
                if (work)
                    work->Cancel();
                downstream_->OnError(error);
            }

            void Dispose()
            {
                std::shared_ptr<IScheduledWork> work;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    stopped_ = true;
                    work = work_;
                    work_.reset();
                }
                if (work)
                    work->Cancel();
            }
        };

        std::shared_ptr<IObservable<T>> source_;
        std::chrono::milliseconds period_;
        std::shared_ptr<IScheduler> scheduler_;

    public:
        SampleObservable(std::shared_ptr<IObservable<T>> source,
                         std::chrono::milliseconds period,
                         std::shared_ptr<IScheduler> scheduler)
            : source_(source), period_(period), scheduler_(scheduler)
        {
        }

        std::shared_ptr<Subscription> Subscribe(
            std::shared_ptr<IObserver<T>> observer) override
        {
            if (period_.count() <= 0)
            {
                std::invalid_argument error(
                    "Sample period must be positive");
                observer->OnError(error);
                return std::make_shared<Subscription>();
            }
            auto state = std::make_shared<State>(observer);
            std::weak_ptr<State> weak = state;
            state->SetWork(scheduler_->SchedulePeriodic(
                [weak]() {
                    if (auto self = weak.lock())
                        self->SampleLatest();
                },
                period_));
            auto source = source_->Subscribe(CreateObserver<T>(
                [state](const T &value) { state->Next(value); },
                [state]() { state->Complete(); },
                [state](const std::exception &error) { state->Error(error); }));
            return std::make_shared<Subscription>(
                [state, source]() {
                    state->Dispose();
                    source->Dispose();
                });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>>) override {}
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Sample(
        std::shared_ptr<IObservable<T>> source,
        std::chrono::milliseconds period,
        std::shared_ptr<IScheduler> scheduler =
            detail::BackgroundSchedulerShared())
    {
        if (!source)
            throw std::invalid_argument("Sample requires a valid source");
        if (!scheduler)
            throw std::invalid_argument("Sample requires a scheduler");
        return std::make_shared<SampleObservable<T>>(
            source, period, scheduler);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Sample(
        std::shared_ptr<Subject<T>> source,
        std::chrono::milliseconds period,
        std::shared_ptr<IScheduler> scheduler =
            detail::BackgroundSchedulerShared())
    {
        return Sample<T>(
            std::static_pointer_cast<IObservable<T>>(source),
            period, scheduler);
    }
}
