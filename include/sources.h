#ifndef MICRO_REACTIVE_SOURCES_H
#define MICRO_REACTIVE_SOURCES_H

#include "core.h"
#include <functional>
#include <memory>
#include <vector>
#include <chrono>
#include <thread>
#include <algorithm>
#include <mutex>
#include <atomic>
#include <condition_variable>

namespace rx
{
    // =============================================================================
    // EMPTY OBSERVABLE - Returns an observable that sends no items and completes
    // =============================================================================
    template <typename T>
    class EmptyObservable : public IObservable<T>
    {
    public:
        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            observer->OnCompleted();
            return std::make_shared<Subscription>([]() {}); // No-op subscription
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
        }

        ~EmptyObservable() = default;
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Empty()
    {
        return std::make_shared<EmptyObservable<T>>();
    }

    // =============================================================================
    // NEVER OBSERVABLE - Returns an observable that never emits anything
    // =============================================================================
    template <typename T>
    class NeverObservable : public IObservable<T>
    {
    public:
        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            // Never emits anything, never completes
            return std::make_shared<Subscription>([]() {}); // No-op subscription
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
        }

        ~NeverObservable() = default;
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Never()
    {
        return std::make_shared<NeverObservable<T>>();
    }

    // =============================================================================
    // RANGE OBSERVABLE - Returns an observable that sends values in a range
    // =============================================================================
    template <typename T>
    class RangeObservable : public IObservable<T>
    {
    private:
        T first_, last_, step_;

    public:
        RangeObservable(T first, T last, T step)
            : first_(first), last_(last), step_(step)
        {
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            for (auto value = first_; value <= last_; value += step_)
                observer->OnNext(value);
            observer->OnCompleted();
            return std::make_shared<Subscription>([]() {}); // No-op subscription for completed observable
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
        }

        ~RangeObservable() = default;
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Range(T first, T last, T step)
    {
        return std::make_shared<RangeObservable<T>>(first, last, step);
    }

    // Overload for Range with default step of 1
    template <typename T>
    std::shared_ptr<IObservable<T>> Range(T first, T count)
    {
        return std::make_shared<RangeObservable<T>>(first, first + count - 1, T(1));
    }

    // =============================================================================
    // FROMVECTOR OBSERVABLE - Creates an observable from a vector
    // =============================================================================
    template <typename T>
    class FromVectorObservable : public IObservable<T>
    {
    private:
        std::vector<T> values_;

    public:
        FromVectorObservable(const std::vector<T> &values) : values_(values)
        {
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            for (const auto &value : values_)
            {
                observer->OnNext(value);
            }
            observer->OnCompleted();
            return std::make_shared<Subscription>([]() {}); // No-op subscription for completed observable
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
        }

        ~FromVectorObservable() = default;
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> FromVector(const std::vector<T> &values)
    {
        return std::make_shared<FromVectorObservable<T>>(values);
    }

    // =============================================================================
    // CREATE OBSERVABLE - Creates an observable from a function
    // =============================================================================
    template <typename T>
    class CreateObservable : public IObservable<T>
    {
    private:
        std::function<void(std::shared_ptr<IObserver<T>>)> create_;

    public:
        CreateObservable(std::function<void(std::shared_ptr<IObserver<T>>)> create)
            : create_(create)
        {
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            try
            {
                create_(observer);
            }
            catch (const std::exception &e)
            {
                observer->OnError(e);
            }
            return std::make_shared<Subscription>([]() {}); // No-op subscription
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
        }

        ~CreateObservable() = default;
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Create(std::function<void(std::shared_ptr<IObserver<T>>)> create)
    {
        return std::make_shared<CreateObservable<T>>(create);
    }

    // =============================================================================
    // ITERATE OBSERVABLE - Creates an observable from a container
    // =============================================================================
    template <typename T>
    class IterateObservable : public IObservable<T>
    {
    private:
        std::vector<T> values_;

    public:
        template <typename Container>
        IterateObservable(const Container &container)
            : values_(container.begin(), container.end())
        {
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            for (const auto &value : values_)
            {
                observer->OnNext(value);
            }
            observer->OnCompleted();
            return std::make_shared<Subscription>([]() {}); // No-op subscription for completed observable
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
        }

        ~IterateObservable() = default;
    };

    template <typename T, typename Container>
    std::shared_ptr<IObservable<T>> Iterate(const Container &container)
    {
        return std::make_shared<IterateObservable<T>>(container);
    }

    // =============================================================================
    // DEFER OBSERVABLE - Defers observable creation until subscription
    // =============================================================================
    template <typename T>
    class DeferObservable : public IObservable<T>
    {
    private:
        std::function<std::shared_ptr<IObservable<T>>()> factory_;

    public:
        DeferObservable(std::function<std::shared_ptr<IObservable<T>>()> factory)
            : factory_(factory)
        {
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            try
            {
                auto observable = factory_();
                return observable->Subscribe(observer);
            }
            catch (const std::exception &e)
            {
                observer->OnError(e);
                return std::make_shared<Subscription>([]() {}); // No-op subscription on error
            }
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
        }

        ~DeferObservable() = default;
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Defer(std::function<std::shared_ptr<IObservable<T>>()> factory)
    {
        return std::make_shared<DeferObservable<T>>(factory);
    }

    // =============================================================================
    // TIMER OBSERVABLE - Threaded version with proper resource management
    // =============================================================================
    template <typename T = int>
    class TimerObservable : public IObservable<T>, public std::enable_shared_from_this<TimerObservable<T>>
    {
    private:
        std::chrono::milliseconds delay_;
        std::vector<std::shared_ptr<IObserver<T>>> observers_;
        std::thread timer_thread_;
        std::atomic<bool> is_running_;
        std::atomic<bool> should_stop_;
        mutable std::mutex observers_mutex_;
        std::condition_variable stop_cv_;
        std::mutex stop_mutex_;

    public:
        TimerObservable(std::chrono::milliseconds delay)
            : delay_(delay), is_running_(false), should_stop_(false)
        {
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            {
                std::lock_guard<std::mutex> lock(observers_mutex_);
                observers_.push_back(observer);
            }

            if (!is_running_.exchange(true))
            {
                should_stop_ = false;
                timer_thread_ = std::thread([this]()
                                            {
                std::unique_lock<std::mutex> lock(stop_mutex_);
                if (!stop_cv_.wait_for(lock, delay_, [this] { return should_stop_.load(); })) {
                    // Timer completed, notify all observers
                    std::lock_guard<std::mutex> obs_lock(observers_mutex_);
                    for (auto& obs : observers_) {
                        if (obs) {
                            obs->OnNext(T{});
                            obs->OnCompleted();
                        }
                    }
                }
                is_running_ = false; });
            }

            // Return subscription for cleanup
            auto weak_self = std::weak_ptr<TimerObservable<T>>(std::static_pointer_cast<TimerObservable<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                self->UnSubscribe(observer);
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            std::lock_guard<std::mutex> lock(observers_mutex_);
            auto it = std::find(observers_.begin(), observers_.end(), observer);
            if (it != observers_.end())
            {
                observers_.erase(it);
            }
        }

        ~TimerObservable()
        {
            should_stop_ = true;
            stop_cv_.notify_all();
            if (timer_thread_.joinable())
            {
                timer_thread_.join();
            }
        }
    };

    template <typename T = int>
    std::shared_ptr<IObservable<T>> Timer(std::chrono::milliseconds delay)
    {
        return std::make_shared<TimerObservable<T>>(delay);
    }

    // =============================================================================
    // INTERVAL OBSERVABLE - Threaded version with proper resource management
    // =============================================================================
    template <typename T = int>
    class IntervalObservable : public IObservable<T>, public std::enable_shared_from_this<IntervalObservable<T>>
    {
    private:
        std::chrono::milliseconds interval_;
        int count_;
        std::vector<std::shared_ptr<IObserver<T>>> observers_;
        std::thread interval_thread_;
        std::atomic<bool> is_running_;
        std::atomic<bool> should_stop_;
        mutable std::mutex observers_mutex_;
        std::condition_variable stop_cv_;
        std::mutex stop_mutex_;

    public:
        IntervalObservable(std::chrono::milliseconds interval, int count = 5)
            : interval_(interval), count_(count), is_running_(false), should_stop_(false)
        {
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            {
                std::lock_guard<std::mutex> lock(observers_mutex_);
                observers_.push_back(observer);
            }

            if (!is_running_.exchange(true))
            {
                should_stop_ = false;
                interval_thread_ = std::thread([this]()
                                               {
                for (int i = 0; i < count_ && !should_stop_.load(); ++i) {
                    std::unique_lock<std::mutex> lock(stop_mutex_);
                    if (stop_cv_.wait_for(lock, interval_, [this] { return should_stop_.load(); })) {
                        break; // Stop requested
                    }
                    
                    // Notify all observers
                    {
                        std::lock_guard<std::mutex> obs_lock(observers_mutex_);
                        for (auto& obs : observers_) {
                            if (obs) {
                                obs->OnNext(T(i));
                            }
                        }
                    }
                }
                
                // Complete all observers if not stopped
                if (!should_stop_.load()) {
                    std::lock_guard<std::mutex> obs_lock(observers_mutex_);
                    for (auto& obs : observers_) {
                        if (obs) {
                            obs->OnCompleted();
                        }
                    }
                }
                is_running_ = false; });
            }

            // Return subscription for cleanup
            auto weak_self = std::weak_ptr<IntervalObservable<T>>(std::static_pointer_cast<IntervalObservable<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                self->UnSubscribe(observer);
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            std::lock_guard<std::mutex> lock(observers_mutex_);
            auto it = std::find(observers_.begin(), observers_.end(), observer);
            if (it != observers_.end())
            {
                observers_.erase(it);
            }
        }

        ~IntervalObservable()
        {
            should_stop_ = true;
            stop_cv_.notify_all();
            if (interval_thread_.joinable())
            {
                interval_thread_.join();
            }
        }
    };

    template <typename T = int>
    std::shared_ptr<IObservable<T>> Interval(std::chrono::milliseconds interval, int count = 5)
    {
        return std::make_shared<IntervalObservable<T>>(interval, count);
    }

} // namespace rx

#endif // MICRO_REACTIVE_SOURCES_H
