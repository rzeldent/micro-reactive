#ifndef MICRO_REACTIVE_OPERATORS_H
#define MICRO_REACTIVE_OPERATORS_H

#include "core.h"
#include <functional>
#include <memory>
#include <vector>
#include <cstddef>

namespace rx {

// =============================================================================
// MAP OPERATOR - Transforms each emitted item by applying a function
// =============================================================================
template <typename Tsrc, typename Tdest>
class MapOperator : public Operator<Tdest> {
    class MapObserver : public IObserver<Tsrc> {
    private:
        Operator<Tdest> *operator_;
        std::function<Tdest(const Tsrc &)> transform_;

    public:
        MapObserver(Operator<Tdest> *op, std::function<Tdest(const Tsrc &)> transform)
            : operator_(op), transform_(transform) {
        }

        void OnNext(const Tsrc &value) override {
            operator_->NotifyOnNext(transform_(value));
        }

        void OnCompleted() override {
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<Tsrc>> observable_;
    std::shared_ptr<MapObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    MapOperator(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform)
        : observable_(observable) {
        observer_ = std::make_shared<MapObserver>(this, transform);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<Tdest>> observer) override {
        auto subscription = Operator<Tdest>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<MapOperator<Tsrc, Tdest>>(
            std::static_pointer_cast<MapOperator<Tsrc, Tdest>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<Tdest>> observer) override {
        Operator<Tdest>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename Tsrc, typename Tdest>
std::shared_ptr<MapOperator<Tsrc, Tdest>> Map(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform) {
    return std::make_shared<MapOperator<Tsrc, Tdest>>(observable, transform);
}

// =============================================================================
// FILTER OPERATOR - Only emits items that pass a predicate test
// =============================================================================
template <typename T>
class FilterOperator : public Operator<T> {
    class FilterObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        std::function<bool(const T &)> predicate_;

    public:
        FilterObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
            : operator_(op), predicate_(predicate) {
        }

        void OnNext(const T &value) override {
            if (predicate_(value))
                operator_->NotifyOnNext(value);
        }

        void OnCompleted() override {
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<FilterObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    FilterOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
        : observable_(observable) {
        observer_ = std::make_shared<FilterObserver>(this, predicate);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<FilterOperator<T>>(
            std::static_pointer_cast<FilterOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<FilterOperator<T>> Filter(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate) {
    return std::make_shared<FilterOperator<T>>(observable, predicate);
}

// =============================================================================
// TAKE OPERATOR - Emits only the first n items
// =============================================================================
template <typename T>
class TakeOperator : public Operator<T> {
    class TakeObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        size_t count_;
        size_t taken_ = 0;

    public:
        TakeObserver(Operator<T> *op, size_t count)
            : operator_(op), count_(count) {
        }

        void OnNext(const T &value) override {
            if (taken_ < count_) {
                operator_->NotifyOnNext(value);
                taken_++;
                if (taken_ == count_)
                    operator_->NotifyOnCompleted();
            }
        }

        void OnCompleted() override {
            if (taken_ < count_)
                operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<TakeObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    TakeOperator(std::shared_ptr<IObservable<T>> observable, size_t count)
        : observable_(observable) {
        observer_ = std::make_shared<TakeObserver>(this, count);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<TakeOperator<T>>(
            std::static_pointer_cast<TakeOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<TakeOperator<T>> Take(std::shared_ptr<IObservable<T>> observable, size_t count) {
    return std::make_shared<TakeOperator<T>>(observable, count);
}

// =============================================================================
// SKIP OPERATOR - Skips the first n items
// =============================================================================
template <typename T>
class SkipOperator : public Operator<T> {
    class SkipObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        size_t count_;
        size_t skipped_ = 0;

    public:
        SkipObserver(Operator<T> *op, size_t count)
            : operator_(op), count_(count) {
        }

        void OnNext(const T &value) override {
            if (skipped_ < count_) {
                skipped_++;
            } else {
                operator_->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<SkipObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    SkipOperator(std::shared_ptr<IObservable<T>> observable, size_t count)
        : observable_(observable) {
        observer_ = std::make_shared<SkipObserver>(this, count);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<SkipOperator<T>>(
            std::static_pointer_cast<SkipOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<SkipOperator<T>> Skip(std::shared_ptr<IObservable<T>> observable, size_t count) {
    return std::make_shared<SkipOperator<T>>(observable, count);
}

// =============================================================================
// DISTINCT OPERATOR - Emits only distinct items (removes duplicates)
// =============================================================================
template <typename T>
class DistinctOperator : public Operator<T> {
    class DistinctObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        std::vector<T> seen_;

    public:
        DistinctObserver(Operator<T> *op) : operator_(op) {}

        void OnNext(const T &value) override {
            // Check if we've seen this value before
            bool found = false;
            for (const auto& seen : seen_) {
                if (seen == value) {
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                seen_.push_back(value);
                operator_->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<DistinctObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    DistinctOperator(std::shared_ptr<IObservable<T>> observable)
        : observable_(observable) {
        observer_ = std::make_shared<DistinctObserver>(this);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<DistinctOperator<T>>(
            std::static_pointer_cast<DistinctOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<DistinctOperator<T>> Distinct(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<DistinctOperator<T>>(observable);
}

// =============================================================================
// SCAN OPERATOR - Applies an accumulator function and emits each result
// =============================================================================
template <typename T, typename TAcc>
class ScanOperator : public Operator<TAcc> {
    class ScanObserver : public IObserver<T> {
    private:
        Operator<TAcc> *operator_;
        std::function<TAcc(const TAcc&, const T&)> accumulator_;
        TAcc seed_;
        bool first_ = true;

    public:
        ScanObserver(Operator<TAcc> *op, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator)
            : operator_(op), seed_(seed), accumulator_(accumulator) {}

        void OnNext(const T &value) override {
            if (first_) {
                seed_ = accumulator_(seed_, value);
                first_ = false;
            } else {
                seed_ = accumulator_(seed_, value);
            }
            operator_->NotifyOnNext(seed_);
        }

        void OnCompleted() override {
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<ScanObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    ScanOperator(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator)
        : observable_(observable) {
        observer_ = std::make_shared<ScanObserver>(this, seed, accumulator);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<TAcc>> observer) override {
        auto subscription = Operator<TAcc>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<ScanOperator<T, TAcc>>(
            std::static_pointer_cast<ScanOperator<T, TAcc>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<TAcc>> observer) override {
        Operator<TAcc>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T, typename TAcc>
std::shared_ptr<ScanOperator<T, TAcc>> Scan(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator) {
    return std::make_shared<ScanOperator<T, TAcc>>(observable, seed, accumulator);
}

// =============================================================================
// REDUCE OPERATOR - Applies an accumulator function and emits only the final result
// =============================================================================
template <typename T, typename TAcc>
class ReduceOperator : public Operator<TAcc> {
    class ReduceObserver : public IObserver<T> {
    private:
        Operator<TAcc> *operator_;
        std::function<TAcc(const TAcc&, const T&)> accumulator_;
        TAcc seed_;

    public:
        ReduceObserver(Operator<TAcc> *op, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator)
            : operator_(op), seed_(seed), accumulator_(accumulator) {}

        void OnNext(const T &value) override {
            seed_ = accumulator_(seed_, value);
        }

        void OnCompleted() override {
            operator_->NotifyOnNext(seed_);
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<ReduceObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    ReduceOperator(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator)
        : observable_(observable) {
        observer_ = std::make_shared<ReduceObserver>(this, seed, accumulator);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<TAcc>> observer) override {
        auto subscription = Operator<TAcc>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<ReduceOperator<T, TAcc>>(
            std::static_pointer_cast<ReduceOperator<T, TAcc>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<TAcc>> observer) override {
        Operator<TAcc>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T, typename TAcc>
std::shared_ptr<ReduceOperator<T, TAcc>> Reduce(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator) {
    return std::make_shared<ReduceOperator<T, TAcc>>(observable, seed, accumulator);
}

// =============================================================================
// THROTTLE OPERATOR - Emits an item only if a particular timespan has passed without emitting another item
// Note: Simplified version for embedded systems without complex timing
// =============================================================================
template <typename T>
class ThrottleOperator : public Operator<T> {
    class ThrottleObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        size_t interval_;
        size_t count_ = 0;

    public:
        ThrottleObserver(Operator<T> *op, size_t interval)
            : operator_(op), interval_(interval) {}

        void OnNext(const T &value) override {
            count_++;
            if (count_ % interval_ == 0) {
                operator_->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<ThrottleObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    ThrottleOperator(std::shared_ptr<IObservable<T>> observable, size_t interval)
        : observable_(observable) {
        observer_ = std::make_shared<ThrottleObserver>(this, interval);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<ThrottleOperator<T>>(
            std::static_pointer_cast<ThrottleOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<ThrottleOperator<T>> Throttle(std::shared_ptr<IObservable<T>> observable, size_t interval) {
    return std::make_shared<ThrottleOperator<T>>(observable, interval);
}

// =============================================================================
// FIRST OPERATOR - Emits only the first item
// =============================================================================
template <typename T>
class FirstOperator : public Operator<T> {
    class FirstObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        std::atomic<bool> emitted_{false};

    public:
        FirstObserver(Operator<T> *op) : operator_(op) {}

        void OnNext(const T &value) override {
            bool expected = false;
            if (emitted_.compare_exchange_strong(expected, true)) {
                operator_->NotifyOnNext(value);
                operator_->NotifyOnCompleted();
            }
        }

        void OnCompleted() override {
            if (!emitted_.load()) {
                operator_->NotifyOnCompleted();
            }
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<FirstObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    FirstOperator(std::shared_ptr<IObservable<T>> observable)
        : observable_(observable) {
        observer_ = std::make_shared<FirstObserver>(this);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<FirstOperator<T>>(
            std::static_pointer_cast<FirstOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<FirstOperator<T>> First(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<FirstOperator<T>>(observable);
}

// =============================================================================
// LAST OPERATOR - Emits only the last item
// =============================================================================
template <typename T>
class LastOperator : public Operator<T> {
    class LastObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        T last_value_;
        std::atomic<bool> has_value_{false};
        mutable std::mutex value_mutex_;

    public:
        LastObserver(Operator<T> *op) : operator_(op) {}

        void OnNext(const T &value) override {
            std::lock_guard<std::mutex> lock(value_mutex_);
            last_value_ = value;
            has_value_.store(true);
        }

        void OnCompleted() override {
            if (has_value_.load()) {
                std::lock_guard<std::mutex> lock(value_mutex_);
                operator_->NotifyOnNext(last_value_);
            }
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<LastObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    LastOperator(std::shared_ptr<IObservable<T>> observable)
        : observable_(observable) {
        observer_ = std::make_shared<LastObserver>(this);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<LastOperator<T>>(
            std::static_pointer_cast<LastOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<LastOperator<T>> Last(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<LastOperator<T>>(observable);
}

// =============================================================================
// WHERE OPERATOR - Alias for Filter (common in LINQ)
// =============================================================================
template <typename T>
std::shared_ptr<FilterOperator<T>> Where(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate) {
    return Filter(observable, predicate);
}

// =============================================================================
// SELECT OPERATOR - Alias for Map (common in LINQ)
// =============================================================================
template <typename Tsrc, typename Tdest>
std::shared_ptr<MapOperator<Tsrc, Tdest>> Select(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform) {
    return Map<Tsrc, Tdest>(observable, transform);
}

// =============================================================================
// BUFFER OPERATOR - Groups emitted items into buffers of a specified size
// =============================================================================
template <typename T>
class BufferOperator : public Operator<std::vector<T>> {
    class BufferObserver : public IObserver<T> {
    private:
        Operator<std::vector<T>> *operator_;
        size_t buffer_size_;
        std::vector<T> buffer_;

    public:
        BufferObserver(Operator<std::vector<T>> *op, size_t bufferSize)
            : operator_(op), buffer_size_(bufferSize) {
        }

        void OnNext(const T &value) override {
            buffer_.push_back(value);
            if (buffer_.size() >= buffer_size_) {
                operator_->NotifyOnNext(buffer_);
                buffer_.clear();
            }
        }

        void OnCompleted() override {
            if (!buffer_.empty()) {
                operator_->NotifyOnNext(buffer_);
            }
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<BufferObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    BufferOperator(std::shared_ptr<IObservable<T>> observable, size_t bufferSize)
        : observable_(observable) {
        observer_ = std::make_shared<BufferObserver>(this, bufferSize);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<std::vector<T>>> observer) override {
        auto subscription = Operator<std::vector<T>>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<BufferOperator<T>>(
            std::static_pointer_cast<BufferOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<std::vector<T>>> observer) override {
        Operator<std::vector<T>>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<BufferOperator<T>> Buffer(std::shared_ptr<IObservable<T>> observable, size_t bufferSize) {
    return std::make_shared<BufferOperator<T>>(observable, bufferSize);
}

// =============================================================================
// TAKEWHILE OPERATOR - Takes items while a condition is true
// =============================================================================
template <typename T>
class TakeWhileOperator : public Operator<T> {
    class TakeWhileObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        std::function<bool(const T &)> predicate_;
        std::atomic<bool> completed_{false};

    public:
        TakeWhileObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
            : operator_(op), predicate_(predicate) {
        }

        void OnNext(const T &value) override {
            if (!completed_.load() && predicate_(value)) {
                operator_->NotifyOnNext(value);
            } else if (!completed_.exchange(true)) {
                operator_->NotifyOnCompleted();
            }
        }

        void OnCompleted() override {
            if (!completed_.exchange(true)) {
                operator_->NotifyOnCompleted();
            }
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<TakeWhileObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    TakeWhileOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
        : observable_(observable) {
        observer_ = std::make_shared<TakeWhileObserver>(this, predicate);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<TakeWhileOperator<T>>(
            std::static_pointer_cast<TakeWhileOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<TakeWhileOperator<T>> TakeWhile(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate) {
    return std::make_shared<TakeWhileOperator<T>>(observable, predicate);
}

// =============================================================================
// SKIPWHILE OPERATOR - Skips items while a condition is true
// =============================================================================
template <typename T>
class SkipWhileOperator : public Operator<T> {
    class SkipWhileObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        std::function<bool(const T &)> predicate_;
        std::atomic<bool> skipping_{true};

    public:
        SkipWhileObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
            : operator_(op), predicate_(predicate) {
        }

        void OnNext(const T &value) override {
            if (skipping_.load() && predicate_(value)) {
                return; // Skip this value
            }
            skipping_.store(false); // Stop skipping once condition fails
            operator_->NotifyOnNext(value);
        }

        void OnCompleted() override {
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<SkipWhileObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    SkipWhileOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
        : observable_(observable) {
        observer_ = std::make_shared<SkipWhileObserver>(this, predicate);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<SkipWhileOperator<T>>(
            std::static_pointer_cast<SkipWhileOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<SkipWhileOperator<T>> SkipWhile(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate) {
    return std::make_shared<SkipWhileOperator<T>>(observable, predicate);
}

// =============================================================================
// STARTWITH OPERATOR - Prepends values to the beginning of the sequence
// =============================================================================
template <typename T>
class StartWithOperator : public Operator<T> {
    class StartWithObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;

    public:
        StartWithObserver(Operator<T> *op) : operator_(op) {
        }

        void OnNext(const T &value) override {
            operator_->NotifyOnNext(value);
        }

        void OnCompleted() override {
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<StartWithObserver> observer_;
    std::vector<T> start_values_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    StartWithOperator(std::shared_ptr<IObservable<T>> observable, std::vector<T> startValues)
        : observable_(observable), start_values_(startValues) {
        observer_ = std::make_shared<StartWithObserver>(this);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                // Emit start values first
                for (const auto& value : start_values_) {
                    this->NotifyOnNext(value);
                }
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<StartWithOperator<T>>(
            std::static_pointer_cast<StartWithOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<StartWithOperator<T>> StartWith(std::shared_ptr<IObservable<T>> observable, std::vector<T> startValues) {
    return std::make_shared<StartWithOperator<T>>(observable, startValues);
}

template <typename T>
std::shared_ptr<StartWithOperator<T>> StartWith(std::shared_ptr<IObservable<T>> observable, T startValue) {
    return std::make_shared<StartWithOperator<T>>(observable, std::vector<T>{startValue});
}

// =============================================================================
// DEFAULTIFEMPTY OPERATOR - Emits a default value if the sequence is empty
// =============================================================================
template <typename T>
class DefaultIfEmptyOperator : public Operator<T> {
    class DefaultIfEmptyObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        T default_value_;
        std::atomic<bool> has_emitted_{false};

    public:
        DefaultIfEmptyObserver(Operator<T> *op, T defaultValue)
            : operator_(op), default_value_(defaultValue) {
        }

        void OnNext(const T &value) override {
            has_emitted_.store(true);
            operator_->NotifyOnNext(value);
        }

        void OnCompleted() override {
            if (!has_emitted_.load()) {
                operator_->NotifyOnNext(default_value_);
            }
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<DefaultIfEmptyObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    DefaultIfEmptyOperator(std::shared_ptr<IObservable<T>> observable, T defaultValue)
        : observable_(observable) {
        observer_ = std::make_shared<DefaultIfEmptyObserver>(this, defaultValue);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<DefaultIfEmptyOperator<T>>(
            std::static_pointer_cast<DefaultIfEmptyOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<DefaultIfEmptyOperator<T>> DefaultIfEmpty(std::shared_ptr<IObservable<T>> observable, T defaultValue) {
    return std::make_shared<DefaultIfEmptyOperator<T>>(observable, defaultValue);
}

// =============================================================================
// COUNT OPERATOR - Counts the number of items emitted
// =============================================================================
template <typename T>
class CountOperator : public Operator<size_t> {
    class CountObserver : public IObserver<T> {
    private:
        Operator<size_t> *operator_;
        std::atomic<size_t> count_{0};

    public:
        CountObserver(Operator<size_t> *op) : operator_(op) {
        }

        void OnNext(const T &value) override {
            count_.fetch_add(1);
        }

        void OnCompleted() override {
            operator_->NotifyOnNext(count_.load());
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<CountObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    CountOperator(std::shared_ptr<IObservable<T>> observable)
        : observable_(observable) {
        observer_ = std::make_shared<CountObserver>(this);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<size_t>> observer) override {
        auto subscription = Operator<size_t>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<CountOperator<T>>(
            std::static_pointer_cast<CountOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<size_t>> observer) override {
        Operator<size_t>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<CountOperator<T>> Count(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<CountOperator<T>>(observable);
}

// =============================================================================
// SUM OPERATOR - Calculates the sum of numeric items
// =============================================================================
template <typename T>
class SumOperator : public Operator<T> {
    class SumObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        T sum_;
        mutable std::mutex sum_mutex_;

    public:
        SumObserver(Operator<T> *op) : operator_(op), sum_(T{}) {
        }

        void OnNext(const T &value) override {
            std::lock_guard<std::mutex> lock(sum_mutex_);
            sum_ += value;
        }

        void OnCompleted() override {
            std::lock_guard<std::mutex> lock(sum_mutex_);
            operator_->NotifyOnNext(sum_);
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<SumObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    SumOperator(std::shared_ptr<IObservable<T>> observable)
        : observable_(observable) {
        observer_ = std::make_shared<SumObserver>(this);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<SumOperator<T>>(
            std::static_pointer_cast<SumOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<SumOperator<T>> Sum(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<SumOperator<T>>(observable);
}

// =============================================================================
// MIN OPERATOR - Finds the minimum value
// =============================================================================
template <typename T>
class MinOperator : public Operator<T> {
    class MinObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        T min_;
        std::atomic<bool> has_value_{false};
        mutable std::mutex value_mutex_;

    public:
        MinObserver(Operator<T> *op) : operator_(op), min_(T{}) {
        }

        void OnNext(const T &value) override {
            std::lock_guard<std::mutex> lock(value_mutex_);
            if (!has_value_.load() || value < min_) {
                min_ = value;
                has_value_.store(true);
            }
        }

        void OnCompleted() override {
            if (has_value_.load()) {
                std::lock_guard<std::mutex> lock(value_mutex_);
                operator_->NotifyOnNext(min_);
            }
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<MinObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    MinOperator(std::shared_ptr<IObservable<T>> observable)
        : observable_(observable) {
        observer_ = std::make_shared<MinObserver>(this);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<MinOperator<T>>(
            std::static_pointer_cast<MinOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<MinOperator<T>> Min(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<MinOperator<T>>(observable);
}

// =============================================================================
// MAX OPERATOR - Finds the maximum value
// =============================================================================
template <typename T>
class MaxOperator : public Operator<T> {
    class MaxObserver : public IObserver<T> {
    private:
        Operator<T> *operator_;
        T max_;
        std::atomic<bool> has_value_{false};
        mutable std::mutex value_mutex_;

    public:
        MaxObserver(Operator<T> *op) : operator_(op), max_(T{}) {
        }

        void OnNext(const T &value) override {
            std::lock_guard<std::mutex> lock(value_mutex_);
            if (!has_value_.load() || value > max_) {
                max_ = value;
                has_value_.store(true);
            }
        }

        void OnCompleted() override {
            if (has_value_.load()) {
                std::lock_guard<std::mutex> lock(value_mutex_);
                operator_->NotifyOnNext(max_);
            }
            operator_->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            operator_->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> observable_;
    std::shared_ptr<MaxObserver> observer_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex subscription_mutex_;

public:
    MaxOperator(std::shared_ptr<IObservable<T>> observable)
        : observable_(observable) {
        observer_ = std::make_shared<MaxObserver>(this);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.size() == 1 && !source_subscription_) {
                source_subscription_ = observable_->Subscribe(observer_);
            }
        }
        
        // Return a subscription that manages both the operator subscription and source subscription
        auto weak_self = std::weak_ptr<MaxOperator<T>>(
            std::static_pointer_cast<MaxOperator<T>>(this->shared_from_this()));
        return std::make_shared<Subscription>([weak_self, subscription, observer]() {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            }
        });
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        std::lock_guard<std::mutex> lock(subscription_mutex_);
        if (this->child_observers_.empty() && source_subscription_) {
            source_subscription_->Dispose();
            source_subscription_.reset();
        }
    }
};

template <typename T>
std::shared_ptr<MaxOperator<T>> Max(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<MaxOperator<T>>(observable);
}

} // namespace rx

#endif // MICRO_REACTIVE_OPERATORS_H

