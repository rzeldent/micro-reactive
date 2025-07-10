#ifndef MICRO_REACTIVE_OPERATORS_H
#define MICRO_REACTIVE_OPERATORS_H

#include "core.h"
#include "scheduler.h"
#include "performance.h"
#include <functional>
#include <memory>
#include <vector>
#include <cstddef>
#include <chrono>
#include <queue>
#include <queue>
#include <set>
#include <atomic>
#include <string>
#include <map>

namespace rx
{

    // =============================================================================
    // MAP OPERATOR - Transforms each emitted item by applying a function
    // =============================================================================
    template <typename Tsrc, typename Tdest>
    class MapOperator : public Operator<Tdest>
    {
        class MapObserver : public IObserver<Tsrc>
        {
        private:
            Operator<Tdest> *operator_;
            std::function<Tdest(const Tsrc &)> transform_;

        public:
            MapObserver(Operator<Tdest> *op, std::function<Tdest(const Tsrc &)> transform)
                : operator_(op), transform_(transform)
            {
            }

            void OnNext(const Tsrc &value) override
            {
                operator_->NotifyOnNext(transform_(value));
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<Tsrc>> observable_;
        std::shared_ptr<MapObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        MapOperator(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform)
            : observable_(observable)
        {
            observer_ = std::make_shared<MapObserver>(this, transform);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<Tdest>> observer) override
        {
            auto subscription = Operator<Tdest>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<MapOperator<Tsrc, Tdest>>(
                std::static_pointer_cast<MapOperator<Tsrc, Tdest>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<Tdest>> observer) override
        {
            Operator<Tdest>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    template <typename Tsrc, typename Tdest>
    std::shared_ptr<IObservable<Tdest>> Map(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform)
    {
        return std::make_shared<MapOperator<Tsrc, Tdest>>(observable, transform);
    }

    // =============================================================================
    // FILTER OPERATOR - Only emits items that pass a predicate test
    // =============================================================================
    template <typename T>
    class FilterOperator : public Operator<T>
    {
        class FilterObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::function<bool(const T &)> predicate_;

        public:
            FilterObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
                : operator_(op), predicate_(predicate)
            {
            }

            void OnNext(const T &value) override
            {
                if (predicate_(value))
                    operator_->NotifyOnNext(value);
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<FilterObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        FilterOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
            : observable_(observable)
        {
            observer_ = std::make_shared<FilterObserver>(this, predicate);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<FilterOperator<T>>(
                std::static_pointer_cast<FilterOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Filter(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
    {
        return std::make_shared<FilterOperator<T>>(observable, predicate);
    }

    // =============================================================================
    // TAKE OPERATOR - Emits only the first n items
    // =============================================================================
    template <typename T>
    class TakeOperator : public Operator<T>
    {
        class TakeObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            size_t count_;
            size_t emitted_;

        public:
            TakeObserver(Operator<T> *op, size_t count) : operator_(op), count_(count), emitted_(0) {}

            void OnNext(const T &value) override
            {
                if (emitted_ < count_)
                {
                    operator_->NotifyOnNext(value);
                    emitted_++;
                    if (emitted_ >= count_)
                    {
                        operator_->NotifyOnCompleted();
                    }
                }
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<TakeObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        TakeOperator(std::shared_ptr<IObservable<T>> observable, size_t count) : observable_(observable)
        {
            observer_ = std::make_shared<TakeObserver>(this, count);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<TakeOperator<T>>(
                std::static_pointer_cast<TakeOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Take(std::shared_ptr<IObservable<T>> observable, size_t count)
    {
        return std::make_shared<TakeOperator<T>>(observable, count);
    }

    // =============================================================================
    // SKIP OPERATOR - Skips the first n items
    // =============================================================================
    template <typename T>
    class SkipOperator : public Operator<T>
    {
        class SkipObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            size_t count_;
            size_t skipped_;

        public:
            SkipObserver(Operator<T> *op, size_t count) : operator_(op), count_(count), skipped_(0) {}

            void OnNext(const T &value) override
            {
                if (skipped_ >= count_)
                {
                    operator_->NotifyOnNext(value);
                }
                else
                {
                    skipped_++;
                }
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<SkipObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        SkipOperator(std::shared_ptr<IObservable<T>> observable, size_t count) : observable_(observable)
        {
            observer_ = std::make_shared<SkipObserver>(this, count);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<SkipOperator<T>>(
                std::static_pointer_cast<SkipOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Skip(std::shared_ptr<IObservable<T>> observable, size_t count)
    {
        return std::make_shared<SkipOperator<T>>(observable, count);
    }

    // =============================================================================
    // DISTINCT OPERATOR - Emits only distinct items (removes duplicates)
    // =============================================================================
    template <typename T>
    class DistinctOperator : public Operator<T>
    {
        class DistinctObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::vector<T> seen_;

        public:
            DistinctObserver(Operator<T> *op) : operator_(op) {}

            void OnNext(const T &value) override
            {
                bool found = false;
                for (const auto &seen : seen_)
                {
                    if (seen == value)
                    {
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    seen_.push_back(value);
                    operator_->NotifyOnNext(value);
                }
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<DistinctObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        DistinctOperator(std::shared_ptr<IObservable<T>> observable) : observable_(observable)
        {
            observer_ = std::make_shared<DistinctObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<DistinctOperator<T>>(
                std::static_pointer_cast<DistinctOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Distinct(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<DistinctOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Distinct(std::shared_ptr<Subject<T>> subject)
    {
        return Distinct(std::static_pointer_cast<IObservable<T>>(subject));
    }

    // =============================================================================
    // DO/TAP OPERATOR - Performs a side effect for each value emitted
    // =============================================================================
    template <typename T>
    class DoOperator : public Operator<T>
    {
        class DoObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::function<void(const T &)> action_;

        public:
            DoObserver(Operator<T> *op, std::function<void(const T &)> action)
                : operator_(op), action_(action)
            {
            }

            void OnNext(const T &value) override
            {
                action_(value);                 // Perform side effect
                operator_->NotifyOnNext(value); // Pass value through
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<DoObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        DoOperator(std::shared_ptr<IObservable<T>> observable, std::function<void(const T &)> action)
            : observable_(observable)
        {
            observer_ = std::make_shared<DoObserver>(this, action);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<DoOperator<T>>(
                std::static_pointer_cast<DoOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for Do operator
    template <typename T>
    std::shared_ptr<IObservable<T>> Do(std::shared_ptr<IObservable<T>> observable, std::function<void(const T &)> action)
    {
        return std::make_shared<DoOperator<T>>(observable, action);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Do(std::shared_ptr<Subject<T>> subject, std::function<void(const T &)> action)
    {
        return Do(std::static_pointer_cast<IObservable<T>>(subject), action);
    }

    // =============================================================================
    // CONTAINS OPERATOR - Emits true if the source contains the specified value
    // =============================================================================
    template <typename T>
    class ContainsOperator : public Operator<bool>
    {
        class ContainsObserver : public IObserver<T>
        {
        private:
            Operator<bool> *operator_;
            T value_;
            bool found_;

        public:
            ContainsObserver(Operator<bool> *op, const T &value)
                : operator_(op), value_(value), found_(false)
            {
            }

            void OnNext(const T &value) override
            {
                if (!found_ && value == value_)
                {
                    found_ = true;
                    operator_->NotifyOnNext(true);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnCompleted() override
            {
                if (!found_)
                {
                    operator_->NotifyOnNext(false);
                }
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<ContainsObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        ContainsOperator(std::shared_ptr<IObservable<T>> observable, const T &value)
            : observable_(observable)
        {
            observer_ = std::make_shared<ContainsObserver>(this, value);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<bool>> observer) override
        {
            auto subscription = Operator<bool>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<ContainsOperator<T>>(
                std::static_pointer_cast<ContainsOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<bool>> observer) override
        {
            Operator<bool>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for Contains operator
    template <typename T>
    std::shared_ptr<IObservable<bool>> Contains(std::shared_ptr<IObservable<T>> observable, const T& value)
    {
        return std::make_shared<ContainsOperator<T>>(observable, value);
    }

    template <typename T>
    std::shared_ptr<IObservable<bool>> Contains(std::shared_ptr<Subject<T>> subject, const T& value)
    {
        return Contains(std::static_pointer_cast<IObservable<T>>(subject), value);
    }

    // =============================================================================
    // TAKEUNTIL OPERATOR - Takes items until a trigger observable emits
    // =============================================================================
    template <typename T, typename TriggerType>
    class TakeUntilOperator : public Operator<T>
    {
        class TakeUntilObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::atomic<bool> completed_;

        public:
            TakeUntilObserver(Operator<T> *op) : operator_(op), completed_(false)
            {
            }

            void OnNext(const T &value) override
            {
                if (!completed_.load())
                {
                    operator_->NotifyOnNext(value);
                }
            }

            void OnCompleted() override
            {
                if (!completed_.exchange(true))
                {
                    operator_->NotifyOnCompleted();
                }
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }

            void TriggerCompleted()
            {
                if (!completed_.exchange(true))
                {
                    operator_->NotifyOnCompleted();
                }
            }
        };

        class TriggerObserver : public IObserver<TriggerType>
        {
        private:
            TakeUntilObserver *observer_;

        public:
            TriggerObserver(TakeUntilObserver *obs) : observer_(obs) {}

            void OnNext(const TriggerType &) override
            {
                observer_->TriggerCompleted();
            }

            void OnCompleted() override
            {
                observer_->TriggerCompleted();
            }

            void OnError(const std::exception &e) override
            {
                // Propagate trigger errors to main stream
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<IObservable<TriggerType>> trigger_;
        std::shared_ptr<TakeUntilObserver> observer_;
        std::shared_ptr<TriggerObserver> trigger_observer_;
        std::shared_ptr<Subscription> source_subscription_;
        std::shared_ptr<Subscription> trigger_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        TakeUntilOperator(std::shared_ptr<IObservable<T>> observable, std::shared_ptr<IObservable<TriggerType>> trigger)
            : observable_(observable), trigger_(trigger)
        {
            observer_ = std::make_shared<TakeUntilObserver>(this);
            trigger_observer_ = std::make_shared<TriggerObserver>(observer_.get());
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                    trigger_subscription_ = trigger_->Subscribe(trigger_observer_);
                }
            }

            auto weak_self = std::weak_ptr<TakeUntilOperator<T, TriggerType>>(
                std::static_pointer_cast<TakeUntilOperator<T, TriggerType>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty()) {
                    if (self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                    if (self->trigger_subscription_) {
                        self->trigger_subscription_->Dispose();
                        self->trigger_subscription_.reset();
                    }
                }
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty())
            {
                if (source_subscription_)
                {
                    source_subscription_->Dispose();
                    source_subscription_.reset();
                }
                if (trigger_subscription_)
                {
                    trigger_subscription_->Dispose();
                    trigger_subscription_.reset();
                }
            }
        }
    };

    // Factory function for TakeUntil operator
    template <typename T, typename TriggerType>
    std::shared_ptr<IObservable<T>> TakeUntil(std::shared_ptr<IObservable<T>> observable, std::shared_ptr<IObservable<TriggerType>> trigger)
    {
        return std::make_shared<TakeUntilOperator<T, TriggerType>>(observable, trigger);
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<IObservable<T>> TakeUntil(std::shared_ptr<Subject<T>> subject, std::shared_ptr<IObservable<TriggerType>> trigger)
    {
        return TakeUntil(std::static_pointer_cast<IObservable<T>>(subject), trigger);
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<IObservable<T>> TakeUntil(std::shared_ptr<IObservable<T>> observable, std::shared_ptr<Subject<TriggerType>> trigger_subject)
    {
        return TakeUntil(observable, std::static_pointer_cast<IObservable<TriggerType>>(trigger_subject));
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<IObservable<T>> TakeUntil(std::shared_ptr<Subject<T>> subject, std::shared_ptr<Subject<TriggerType>> trigger_subject)
    {
        return TakeUntil(std::static_pointer_cast<IObservable<T>>(subject), std::static_pointer_cast<IObservable<TriggerType>>(trigger_subject));
    }

    // =============================================================================
    // SKIPUNTIL OPERATOR - Skips items until a trigger observable emits
    // =============================================================================
    template <typename T, typename TriggerType>
    class SkipUntilOperator : public Operator<T>
    {
        class SkipUntilObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::atomic<bool> triggered_;

        public:
            SkipUntilObserver(Operator<T> *op) : operator_(op), triggered_(false)
            {
            }

            void OnNext(const T &value) override
            {
                if (triggered_.load())
                {
                    operator_->NotifyOnNext(value);
                }
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }

            void SetTriggered()
            {
                triggered_.store(true);
            }
        };

        class TriggerObserver : public IObserver<TriggerType>
        {
        private:
            SkipUntilObserver *observer_;

        public:
            TriggerObserver(SkipUntilObserver *obs) : observer_(obs) {}

            void OnNext(const TriggerType &) override
            {
                observer_->SetTriggered();
            }

            void OnCompleted() override
            {
                observer_->SetTriggered();
            }

            void OnError(const std::exception &e) override
            {
                // Trigger errors are generally ignored in SkipUntil
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<IObservable<TriggerType>> trigger_;
        std::shared_ptr<SkipUntilObserver> observer_;
        std::shared_ptr<TriggerObserver> trigger_observer_;
        std::shared_ptr<Subscription> source_subscription_;
        std::shared_ptr<Subscription> trigger_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        SkipUntilOperator(std::shared_ptr<IObservable<T>> observable, std::shared_ptr<IObservable<TriggerType>> trigger)
            : observable_(observable), trigger_(trigger)
        {
            observer_ = std::make_shared<SkipUntilObserver>(this);
            trigger_observer_ = std::make_shared<TriggerObserver>(observer_.get());
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                    trigger_subscription_ = trigger_->Subscribe(trigger_observer_);
                }
            }

            auto weak_self = std::weak_ptr<SkipUntilOperator<T, TriggerType>>(
                std::static_pointer_cast<SkipUntilOperator<T, TriggerType>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty()) {
                    if (self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                    if (self->trigger_subscription_) {
                        self->trigger_subscription_->Dispose();
                        self->trigger_subscription_.reset();
                    }
                }
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty())
            {
                if (source_subscription_)
                {
                    source_subscription_->Dispose();
                    source_subscription_.reset();
                }
                if (trigger_subscription_)
                {
                    trigger_subscription_->Dispose();
                    trigger_subscription_.reset();
                }
            }
        }
    };

    // Factory function for SkipUntil operator
    template <typename T, typename TriggerType>
    std::shared_ptr<IObservable<T>> SkipUntil(std::shared_ptr<IObservable<T>> observable, std::shared_ptr<IObservable<TriggerType>> trigger)
    {
        return std::make_shared<SkipUntilOperator<T, TriggerType>>(observable, trigger);
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<IObservable<T>> SkipUntil(std::shared_ptr<Subject<T>> subject, std::shared_ptr<IObservable<TriggerType>> trigger)
    {
        return SkipUntil(std::static_pointer_cast<IObservable<T>>(subject), trigger);
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<IObservable<T>> SkipUntil(std::shared_ptr<IObservable<T>> observable, std::shared_ptr<Subject<TriggerType>> trigger_subject)
    {
        return SkipUntil(observable, std::static_pointer_cast<IObservable<TriggerType>>(trigger_subject));
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<IObservable<T>> SkipUntil(std::shared_ptr<Subject<T>> subject, std::shared_ptr<Subject<TriggerType>> trigger_subject)
    {
        return SkipUntil(std::static_pointer_cast<IObservable<T>>(subject), std::static_pointer_cast<IObservable<TriggerType>>(trigger_subject));
    }

    // =============================================================================
    // DISTINCT UNTIL CHANGED OPERATOR - Only emit items when they differ from the previous item
    // =============================================================================
    template <typename T>
    class DistinctUntilChangedOperator : public Operator<T>
    {
        class DistinctUntilChangedObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            bool has_previous_;
            T previous_value_;

        public:
            DistinctUntilChangedObserver(Operator<T> *op)
                : operator_(op), has_previous_(false)
            {
            }

            void OnNext(const T &value) override
            {
                if (!has_previous_ || !(value == previous_value_))
                {
                    has_previous_ = true;
                    previous_value_ = value;
                    operator_->NotifyOnNext(value);
                }
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<DistinctUntilChangedObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        DistinctUntilChangedOperator(std::shared_ptr<IObservable<T>> observable)
            : observable_(observable)
        {
            observer_ = std::make_shared<DistinctUntilChangedObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<DistinctUntilChangedOperator<T>>(
                std::static_pointer_cast<DistinctUntilChangedOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for DistinctUntilChanged operator
    template <typename T>
    std::shared_ptr<IObservable<T>> DistinctUntilChanged(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<DistinctUntilChangedOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> DistinctUntilChanged(std::shared_ptr<Subject<T>> subject)
    {
        return DistinctUntilChanged(std::static_pointer_cast<IObservable<T>>(subject));
    }

    // =============================================================================
    // PAIRWISE OPERATOR - Emits the previous and current value as a pair
    // =============================================================================
    template <typename T>
    class PairwiseOperator : public Operator<std::pair<T, T>>
    {
        class PairwiseObserver : public IObserver<T>
        {
        private:
            Operator<std::pair<T, T>> *operator_;
            bool has_previous_;
            T previous_value_;

        public:
            PairwiseObserver(Operator<std::pair<T, T>> *op)
                : operator_(op), has_previous_(false)
            {
            }

            void OnNext(const T &value) override
            {
                if (has_previous_)
                {
                    operator_->NotifyOnNext(std::make_pair(previous_value_, value));
                }
                has_previous_ = true;
                previous_value_ = value;
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<PairwiseObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        PairwiseOperator(std::shared_ptr<IObservable<T>> observable)
            : observable_(observable)
        {
            observer_ = std::make_shared<PairwiseObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<std::pair<T, T>>> observer) override
        {
            auto subscription = Operator<std::pair<T, T>>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<PairwiseOperator<T>>(
                std::static_pointer_cast<PairwiseOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                subscription->Dispose();
                std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                if (self->child_observers_.empty() && self->source_subscription_) {
                    self->source_subscription_->Dispose();
                    self->source_subscription_.reset();
                }
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<std::pair<T, T>>> observer) override
        {
            Operator<std::pair<T, T>>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for Pairwise operator
    template <typename T>
    std::shared_ptr<IObservable<std::pair<T, T>>> Pairwise(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<PairwiseOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<IObservable<std::pair<T, T>>> Pairwise(std::shared_ptr<Subject<T>> subject)
    {
        return Pairwise(std::static_pointer_cast<IObservable<T>>(subject));
    }

    // =============================================================================
    // RACE OPERATOR - Emits values from the first observable to emit
    // =============================================================================
    template <typename T>
    class RaceOperator : public Operator<T>
    {
        class RaceObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::atomic<bool> *first_emitted_;
            std::atomic<bool> *is_winner_;

        public:
            RaceObserver(Operator<T> *op, std::atomic<bool> *first_emitted, std::atomic<bool> *is_winner)
                : operator_(op), first_emitted_(first_emitted), is_winner_(is_winner)
            {
            }

            void OnNext(const T &value) override
            {
                bool expected = false;
                if (first_emitted_->compare_exchange_strong(expected, true))
                {
                    // This is the first observable to emit - it becomes the winner
                    is_winner_->store(true);
                    operator_->NotifyOnNext(value);
                }
                else if (is_winner_->load())
                {
                    // This observable already won, so continue emitting
                    operator_->NotifyOnNext(value);
                }
                // If another observable won, ignore this emission
            }

            void OnCompleted() override
            {
                if (is_winner_->load())
                {
                    operator_->NotifyOnCompleted();
                }
            }

            void OnError(const std::exception &e) override
            {
                if (is_winner_->load())
                {
                    operator_->NotifyOnError(e);
                }
            }
        };

        std::vector<std::shared_ptr<IObservable<T>>> observables_;
        std::vector<std::shared_ptr<RaceObserver>> observers_;
        std::vector<std::shared_ptr<Subscription>> source_subscriptions_;
        std::vector<std::atomic<bool>> is_winner_;
        std::atomic<bool> first_emitted_;
        mutable std::mutex subscription_mutex_;

    public:
        RaceOperator(std::vector<std::shared_ptr<IObservable<T>>> observables)
            : observables_(observables), first_emitted_(false), is_winner_(observables.size())
        {
            for (size_t i = 0; i < observables.size(); ++i)
            {
                is_winner_[i].store(false);
                observers_.push_back(std::make_shared<RaceObserver>(this, &first_emitted_, &is_winner_[i]));
            }
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && source_subscriptions_.empty())
                {
                    for (size_t i = 0; i < observables_.size(); ++i)
                    {
                        source_subscriptions_.push_back(observables_[i]->Subscribe(observers_[i]));
                    }
                }
            }

            auto weak_self = std::weak_ptr<RaceOperator<T>>(
                std::static_pointer_cast<RaceOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && !self->source_subscriptions_.empty()) {
                        for (auto& sub : self->source_subscriptions_) {
                            sub->Dispose();
                        }
                        self->source_subscriptions_.clear();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && !source_subscriptions_.empty())
            {
                for (auto &sub : source_subscriptions_)
                {
                    sub->Dispose();
                }
                source_subscriptions_.clear();
            }
        }
    };

    // Factory function for Race operator
    template <typename T>
    std::shared_ptr<IObservable<T>> Race(std::vector<std::shared_ptr<IObservable<T>>> observables)
    {
        return std::make_shared<RaceOperator<T>>(observables);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Race(std::initializer_list<std::shared_ptr<IObservable<T>>> observables)
    {
        return Race(std::vector<std::shared_ptr<IObservable<T>>>(observables));
    }

    // =============================================================================
    // AGGREGATION OPERATORS
    // =============================================================================

    // Count operator - counts the number of emitted items
    template <typename T>
    class CountOperator : public Operator<size_t>
    {
        class CountObserver : public IObserver<T>
        {
        private:
            Operator<size_t> *operator_;
            size_t count_;

        public:
            CountObserver(Operator<size_t> *op) : operator_(op), count_(0) {}

            void OnNext(const T &value) override
            {
                count_++;
            }

            void OnCompleted() override
            {
                operator_->NotifyOnNext(count_);
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<CountObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        CountOperator(std::shared_ptr<IObservable<T>> observable) : observable_(observable)
        {
            observer_ = std::make_shared<CountObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<size_t>> observer) override
        {
            auto subscription = Operator<size_t>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<CountOperator<T>>(
                std::static_pointer_cast<CountOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<size_t>> observer) override
        {
            Operator<size_t>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Sum operator - sums all emitted numeric values
    template <typename T>
    class SumOperator : public Operator<T>
    {
        class SumObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            T sum_;

        public:
            SumObserver(Operator<T> *op) : operator_(op), sum_(T{}) {}

            void OnNext(const T &value) override
            {
                sum_ += value;
            }

            void OnCompleted() override
            {
                operator_->NotifyOnNext(sum_);
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<SumObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        SumOperator(std::shared_ptr<IObservable<T>> observable) : observable_(observable)
        {
            observer_ = std::make_shared<SumObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<SumOperator<T>>(
                std::static_pointer_cast<SumOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Min operator - finds minimum value
    template <typename T>
    class MinOperator : public Operator<T>
    {
        class MinObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            T min_value_;
            bool has_value_;

        public:
            MinObserver(Operator<T> *op) : operator_(op), has_value_(false) {}

            void OnNext(const T &value) override
            {
                if (!has_value_ || value < min_value_)
                {
                    min_value_ = value;
                    has_value_ = true;
                }
            }

            void OnCompleted() override
            {
                if (has_value_)
                {
                    operator_->NotifyOnNext(min_value_);
                }
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<MinObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        MinOperator(std::shared_ptr<IObservable<T>> observable) : observable_(observable)
        {
            observer_ = std::make_shared<MinObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<MinOperator<T>>(
                std::static_pointer_cast<MinOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Max operator - finds maximum value
    template <typename T>
    class MaxOperator : public Operator<T>
    {
        class MaxObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            T max_value_;
            bool has_value_;

        public:
            MaxObserver(Operator<T> *op) : operator_(op), has_value_(false) {}

            void OnNext(const T &value) override
            {
                if (!has_value_ || value > max_value_)
                {
                    max_value_ = value;
                    has_value_ = true;
                }
            }

            void OnCompleted() override
            {
                if (has_value_)
                {
                    operator_->NotifyOnNext(max_value_);
                }
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<MaxObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        MaxOperator(std::shared_ptr<IObservable<T>> observable) : observable_(observable)
        {
            observer_ = std::make_shared<MaxObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<MaxOperator<T>>(
                std::static_pointer_cast<MaxOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Average operator - calculates average of numeric values
    template <typename T>
    class AverageOperator : public Operator<T>
    {
        class AverageObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            T sum_;
            size_t count_;

        public:
            AverageObserver(Operator<T> *op) : operator_(op), sum_(T{}), count_(0) {}

            void OnNext(const T &value) override
            {
                sum_ += value;
                count_++;
            }

            void OnCompleted() override
            {
                if (count_ > 0)
                {
                    T average = sum_ / static_cast<T>(count_);
                    operator_->NotifyOnNext(average);
                }
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<AverageObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        AverageOperator(std::shared_ptr<IObservable<T>> observable) : observable_(observable)
        {
            observer_ = std::make_shared<AverageObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<AverageOperator<T>>(
                std::static_pointer_cast<AverageOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // DefaultIfEmpty operator - emits a default value if source is empty
    template <typename T>
    class DefaultIfEmptyOperator : public Operator<T>
    {
        class DefaultIfEmptyObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            T default_value_;
            bool has_emitted_;

        public:
            DefaultIfEmptyObserver(Operator<T> *op, const T& default_value) 
                : operator_(op), default_value_(default_value), has_emitted_(false) {}

            void OnNext(const T &value) override
            {
                has_emitted_ = true;
                operator_->NotifyOnNext(value);
            }

            void OnCompleted() override
            {
                if (!has_emitted_)
                {
                    operator_->NotifyOnNext(default_value_);
                }
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<DefaultIfEmptyObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        DefaultIfEmptyOperator(std::shared_ptr<IObservable<T>> observable, const T& default_value) 
            : observable_(observable)
        {
            observer_ = std::make_shared<DefaultIfEmptyObserver>(this, default_value);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<DefaultIfEmptyOperator<T>>(
                std::static_pointer_cast<DefaultIfEmptyOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // StartWith operator - prepends values to the beginning of the stream
    template <typename T>
    class StartWithOperator : public Operator<T>
    {
        class StartWithObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;

        public:
            StartWithObserver(Operator<T> *op) : operator_(op) {}

            void OnNext(const T &value) override
            {
                operator_->NotifyOnNext(value);
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::vector<T> start_values_;
        std::shared_ptr<StartWithObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        StartWithOperator(std::shared_ptr<IObservable<T>> observable, const std::vector<T>& start_values) 
            : observable_(observable), start_values_(start_values)
        {
            observer_ = std::make_shared<StartWithObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            // Emit start values first
            for (const auto& value : start_values_)
            {
                this->NotifyOnNext(value);
            }

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<StartWithOperator<T>>(
                std::static_pointer_cast<StartWithOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // =============================================================================
    // DEBUG OPERATOR - Passes through all values while collecting metrics
    // =============================================================================
    template <typename T>
    class DebugOperator : public Operator<T>
    {
        class DebugObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::shared_ptr<ObservableMetrics> metrics_;

        public:
            DebugObserver(Operator<T> *op, std::shared_ptr<ObservableMetrics> metrics)
                : operator_(op), metrics_(metrics)
            {
            }

            void OnNext(const T &value) override
            {
                metrics_->RecordEmission();
                operator_->NotifyOnNext(value);
            }

            void OnCompleted() override
            {
                metrics_->RecordCompletion();
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                metrics_->RecordError();
                operator_->NotifyOnError(e);
            }
        };

    private:
        std::shared_ptr<IObservable<T>> source_;
        std::string name_;
        std::shared_ptr<ObservableMetrics> metrics_;

    public:
        DebugOperator(std::shared_ptr<IObservable<T>> source, const std::string& name)
            : source_(source), name_(name), metrics_(std::make_shared<ObservableMetrics>())
        {
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            metrics_->RecordSubscription();
            // Add observer to our child observers list
            auto subscription = Operator<T>::Subscribe(observer);
            
            // Subscribe to source with our debug observer
            auto debug_observer = std::make_shared<DebugObserver>(this, metrics_);
            auto source_subscription = source_->Subscribe(debug_observer);
            
            // Return a composite subscription that manages both
            return std::make_shared<Subscription>([subscription, source_subscription]() {
                subscription->Dispose();
                source_subscription->Dispose();
            });
        }

        std::shared_ptr<ObservableMetrics> GetMetrics() const
        {
            return metrics_;
        }

        std::string GetName() const
        {
            return name_;
        }
    };

    // Factory function for Debug operator
    template <typename T>
    std::shared_ptr<DebugOperator<T>> Debug(std::shared_ptr<IObservable<T>> observable, const std::string& name)
    {
        return std::make_shared<DebugOperator<T>>(observable, name);
    }

    // Subject overload for Debug operator
    template <typename T>
    std::shared_ptr<DebugOperator<T>> Debug(std::shared_ptr<Subject<T>> subject, const std::string& name)
    {
        return Debug(std::static_pointer_cast<IObservable<T>>(subject), name);
    }

    // =============================================================================
    // AGGREGATION OPERATOR FACTORY FUNCTIONS
    // =============================================================================

    // Factory function for Count operator
    template <typename T>
    std::shared_ptr<CountOperator<T>> Count(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<CountOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<CountOperator<T>> Count(std::shared_ptr<Subject<T>> subject)
    {
        return Count(std::static_pointer_cast<IObservable<T>>(subject));
    }

    // Factory function for Sum operator
    template <typename T>
    std::shared_ptr<SumOperator<T>> Sum(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<SumOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<SumOperator<T>> Sum(std::shared_ptr<Subject<T>> subject)
    {
        return Sum(std::static_pointer_cast<IObservable<T>>(subject));
    }

    // Factory function for Min operator
    template <typename T>
    std::shared_ptr<MinOperator<T>> Min(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<MinOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<MinOperator<T>> Min(std::shared_ptr<Subject<T>> subject)
    {
        return Min(std::static_pointer_cast<IObservable<T>>(subject));
    }

    // Factory function for Max operator
    template <typename T>
    std::shared_ptr<MaxOperator<T>> Max(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<MaxOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<MaxOperator<T>> Max(std::shared_ptr<Subject<T>> subject)
    {
        return Max(std::static_pointer_cast<IObservable<T>>(subject));
    }

    // Factory function for Average operator
    template <typename T>
    std::shared_ptr<AverageOperator<T>> Average(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<AverageOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<AverageOperator<T>> Average(std::shared_ptr<Subject<T>> subject)
    {
        return Average(std::static_pointer_cast<IObservable<T>>(subject));
    }

    // Factory function for DefaultIfEmpty operator
    template <typename T>
    std::shared_ptr<DefaultIfEmptyOperator<T>> DefaultIfEmpty(std::shared_ptr<IObservable<T>> observable, const T& default_value)
    {
        return std::make_shared<DefaultIfEmptyOperator<T>>(observable, default_value);
    }

    template <typename T>
    std::shared_ptr<DefaultIfEmptyOperator<T>> DefaultIfEmpty(std::shared_ptr<Subject<T>> subject, const T& default_value)
    {
        return DefaultIfEmpty(std::static_pointer_cast<IObservable<T>>(subject), default_value);
    }

    // Factory function for StartWith operator
    template <typename T>
    std::shared_ptr<StartWithOperator<T>> StartWith(std::shared_ptr<IObservable<T>> observable, const std::vector<T>& start_values)
    {
        return std::make_shared<StartWithOperator<T>>(observable, start_values);
    }

    template <typename T>
    std::shared_ptr<StartWithOperator<T>> StartWith(std::shared_ptr<Subject<T>> subject, const std::vector<T>& start_values)
    {
        return StartWith(std::static_pointer_cast<IObservable<T>>(subject), start_values);
    }

    // =============================================================================
    // UTILITY AND CONDITIONAL OPERATORS  
    // =============================================================================

    // Scan operator - emits accumulated values
    template <typename T, typename TAcc>
    class ScanOperator : public Operator<TAcc>
    {
        class ScanObserver : public IObserver<T>
        {
        private:
            Operator<TAcc> *operator_;
            TAcc accumulator_;
            std::function<TAcc(const TAcc &, const T &)> accumulatorFunc_;

        public:
            ScanObserver(Operator<TAcc> *op, TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulatorFunc)
                : operator_(op), accumulator_(seed), accumulatorFunc_(accumulatorFunc) {}

            void OnNext(const T &value) override
            {
                accumulator_ = accumulatorFunc_(accumulator_, value);
                operator_->NotifyOnNext(accumulator_);
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<ScanObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        ScanOperator(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulator)
            : observable_(observable)
        {
            observer_ = std::make_shared<ScanObserver>(this, seed, accumulator);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<TAcc>> observer) override
        {
            auto subscription = Operator<TAcc>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<ScanOperator<T, TAcc>>(
                std::static_pointer_cast<ScanOperator<T, TAcc>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<TAcc>> observer) override
        {
            Operator<TAcc>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for Scan operator
    template <typename T, typename TAcc>
    std::shared_ptr<IObservable<TAcc>> Scan(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulator)
    {
        return std::make_shared<ScanOperator<T, TAcc>>(observable, seed, accumulator);
    }

    template <typename T, typename TAcc>
    std::shared_ptr<IObservable<TAcc>> Scan(std::shared_ptr<Subject<T>> subject, TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulator)
    {
        return Scan(std::static_pointer_cast<IObservable<T>>(subject), seed, accumulator);
    }

    // =============================================================================
    // REDUCE OPERATOR - Emits single accumulated result
    // =============================================================================
    template <typename T, typename TAcc>
    class ReduceOperator : public Operator<TAcc>
    {
        class ReduceObserver : public IObserver<T>
        {
        private:
            Operator<TAcc> *operator_;
            TAcc accumulator_;
            std::function<TAcc(const TAcc &, const T &)> accumulatorFunc_;

        public:
            ReduceObserver(Operator<TAcc> *op, TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulatorFunc)
                : operator_(op), accumulator_(seed), accumulatorFunc_(accumulatorFunc) {}

            void OnNext(const T &value) override
            {
                accumulator_ = accumulatorFunc_(accumulator_, value);
            }

            void OnCompleted() override
            {
                operator_->NotifyOnNext(accumulator_);
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<ReduceObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        ReduceOperator(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulator)
            : observable_(observable)
        {
            observer_ = std::make_shared<ReduceObserver>(this, seed, accumulator);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<TAcc>> observer) override
        {
            auto subscription = Operator<TAcc>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<ReduceOperator<T, TAcc>>(
                std::static_pointer_cast<ReduceOperator<T, TAcc>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<TAcc>> observer) override
        {
            Operator<TAcc>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for Reduce operator
    template <typename T, typename TAcc>
    std::shared_ptr<IObservable<TAcc>> Reduce(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulator)
    {
        return std::make_shared<ReduceOperator<T, TAcc>>(observable, seed, accumulator);
    }

    template <typename T, typename TAcc>
    std::shared_ptr<IObservable<TAcc>> Reduce(std::shared_ptr<Subject<T>> subject, TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulator)
    {
        return Reduce(std::static_pointer_cast<IObservable<T>>(subject), seed, accumulator);
    }

    // Throttle operator - emits every nth item
    template <typename T>
    class ThrottleOperator : public Operator<T>
    {
        class ThrottleObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            size_t interval_;
            size_t count_;

        public:
            ThrottleObserver(Operator<T> *op, size_t interval) : operator_(op), interval_(interval), count_(0) {}

            void OnNext(const T &value) override
            {
                count_++;
                if (count_ % interval_ == 0)
                {
                    operator_->NotifyOnNext(value);
                }
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<ThrottleObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        ThrottleOperator(std::shared_ptr<IObservable<T>> observable, size_t interval) : observable_(observable)
        {
            observer_ = std::make_shared<ThrottleObserver>(this, interval);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<ThrottleOperator<T>>(
                std::static_pointer_cast<ThrottleOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for Throttle operator
    template <typename T>
    std::shared_ptr<IObservable<T>> Throttle(std::shared_ptr<IObservable<T>> observable, size_t interval)
    {
        return std::make_shared<ThrottleOperator<T>>(observable, interval);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Throttle(std::shared_ptr<Subject<T>> subject, size_t interval)
    {
        return Throttle(std::static_pointer_cast<IObservable<T>>(subject), interval);
    }

    // First operator - emits only the first item
    template <typename T>
    class FirstOperator : public Operator<T>
    {
        class FirstObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            bool has_emitted_;

        public:
            FirstObserver(Operator<T> *op) : operator_(op), has_emitted_(false) {}

            void OnNext(const T &value) override
            {
                if (!has_emitted_)
                {
                    has_emitted_ = true;
                    operator_->NotifyOnNext(value);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnCompleted() override
            {
                if (!has_emitted_)
                {
                    operator_->NotifyOnCompleted();
                }
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<FirstObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        FirstOperator(std::shared_ptr<IObservable<T>> observable) : observable_(observable)
        {
            observer_ = std::make_shared<FirstObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<FirstOperator<T>>(
                std::static_pointer_cast<FirstOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for First operator
    template <typename T>
    std::shared_ptr<IObservable<T>> First(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<FirstOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> First(std::shared_ptr<Subject<T>> subject)
    {
        return First(std::static_pointer_cast<IObservable<T>>(subject));
    }

    // Last operator - emits only the last item  
    template <typename T>
    class LastOperator : public Operator<T>
    {
        class LastObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            T last_value_;
            bool has_value_;

        public:
            LastObserver(Operator<T> *op) : operator_(op), has_value_(false) {}

            void OnNext(const T &value) override
            {

                last_value_ = value;
                has_value_ = true;
            }

            void OnCompleted() override
            {
                if (has_value_)
                {
                    operator_->NotifyOnNext(last_value_);
                }
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<LastObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        LastOperator(std::shared_ptr<IObservable<T>> observable) : observable_(observable)
        {
            observer_ = std::make_shared<LastObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<LastOperator<T>>(
                std::static_pointer_cast<LastOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for Last operator
    template <typename T>
    std::shared_ptr<IObservable<T>> Last(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<LastOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Last(std::shared_ptr<Subject<T>> subject)
    {
        return Last(std::static_pointer_cast<IObservable<T>>(subject));
    }

    // TakeWhile operator - emits items while predicate is true
    template <typename T>
    class TakeWhileOperator : public Operator<T>
    {
        class TakeWhileObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::function<bool(const T &)> predicate_;

        public:
            TakeWhileObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
                : operator_(op), predicate_(predicate) {}

            void OnNext(const T &value) override
            {
                if (predicate_(value))
                {
                    operator_->NotifyOnNext(value);
                }
                else
                {
                    operator_->NotifyOnCompleted();
                }
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<TakeWhileObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        TakeWhileOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
            : observable_(observable)
        {
            observer_ = std::make_shared<TakeWhileObserver>(this, predicate);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<TakeWhileOperator<T>>(
                std::static_pointer_cast<TakeWhileOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for TakeWhile operator
    template <typename T>
    std::shared_ptr<IObservable<T>> TakeWhile(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
    {
        return std::make_shared<TakeWhileOperator<T>>(observable, predicate);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> TakeWhile(std::shared_ptr<Subject<T>> subject, std::function<bool(const T &)> predicate)
    {
        return TakeWhile(std::static_pointer_cast<IObservable<T>>(subject), predicate);
    }

    // SkipWhile operator - skips items while predicate is true
    template <typename T>
    class SkipWhileOperator : public Operator<T>
    {
        class SkipWhileObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::function<bool(const T &)> predicate_;
            bool should_skip_;

        public:
            SkipWhileObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
                : operator_(op), predicate_(predicate), should_skip_(true) {}

            void OnNext(const T &value) override
            {
                if (should_skip_ && predicate_(value))
                {
                    return; // Skip this value
                }
                else
                {
                    should_skip_ = false;
                    operator_->NotifyOnNext(value);
                }
            }

            void OnCompleted() override
            {
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<SkipWhileObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        SkipWhileOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
            : observable_(observable)
        {
            observer_ = std::make_shared<SkipWhileObserver>(this, predicate);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<SkipWhileOperator<T>>(
                std::static_pointer_cast<SkipWhileOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            Operator<T>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for SkipWhile operator
    template <typename T>
    std::shared_ptr<IObservable<T>> SkipWhile(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
    {
        return std::make_shared<SkipWhileOperator<T>>(observable, predicate);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> SkipWhile(std::shared_ptr<Subject<T>> subject, std::function<bool(const T &)> predicate)
    {
        return SkipWhile(std::static_pointer_cast<IObservable<T>>(subject), predicate);
    }

    // All operator - checks if all items satisfy predicate
    template <typename T>
    class AllOperator : public Operator<bool>
    {
        class AllObserver : public IObserver<T>
        {
        private:
            Operator<bool> *operator_;
            std::function<bool(const T &)> predicate_;
            bool all_true_;

        public:
            AllObserver(Operator<bool> *op, std::function<bool(const T &)> predicate)
                : operator_(op), predicate_(predicate), all_true_(true) {}

            void OnNext(const T &value) override
            {
                if (all_true_ && !predicate_(value))
                {
                    all_true_ = false;
                    operator_->NotifyOnNext(false);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnCompleted() override
            {
                if (all_true_)
                {
                    operator_->NotifyOnNext(true);
                }
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<AllObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        AllOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
            : observable_(observable)
        {
            observer_ = std::make_shared<AllObserver>(this, predicate);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<bool>> observer) override
        {
            auto subscription = Operator<bool>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<AllOperator<T>>(
                std::static_pointer_cast<AllOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<bool>> observer) override
        {
            Operator<bool>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for All operator
    template <typename T>
    std::shared_ptr<IObservable<bool>> All(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
    {
        return std::make_shared<AllOperator<T>>(observable, predicate);
    }

    template <typename T>
    std::shared_ptr<IObservable<bool>> All(std::shared_ptr<Subject<T>> subject, std::function<bool(const T &)> predicate)
    {
        return All(std::static_pointer_cast<IObservable<T>>(subject), predicate);
    }

    // Any operator - checks if any item satisfies predicate
    template <typename T>
    class AnyOperator : public Operator<bool>
    {
        class AnyObserver : public IObserver<T>
        {
        private:
            Operator<bool> *operator_;
            std::function<bool(const T &)> predicate_;
            bool found_;

        public:
            AnyObserver(Operator<bool> *op, std::function<bool(const T &)> predicate)
                : operator_(op), predicate_(predicate), found_(false) {}

            void OnNext(const T &value) override
            {
                if (!found_ && predicate_(value))
                {
                    found_ = true;
                    operator_->NotifyOnNext(true);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnCompleted() override
            {
                if (!found_)
                {
                    operator_->NotifyOnNext(false);
                }
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<AnyObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        AnyOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
            : observable_(observable)
        {
            observer_ = std::make_shared<AnyObserver>(this, predicate);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<bool>> observer) override
        {
            auto subscription = Operator<bool>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<AnyOperator<T>>(
                std::static_pointer_cast<AnyOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_.reset();
                    }
                } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<bool>> observer) override
        {
            Operator<bool>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    // Factory function for Any operator
    template <typename T>
    std::shared_ptr<IObservable<bool>> Any(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
    {
        return std::make_shared<AnyOperator<T>>(observable, predicate);
    }

    template <typename T>
    std::shared_ptr<IObservable<bool>> Any(std::shared_ptr<Subject<T>> subject, std::function<bool(const T &)> predicate)
    {
        return Any(std::static_pointer_cast<IObservable<T>>(subject), predicate);
    }

    // =============================================================================
}

#endif // MICRO_REACTIVE_OPERATORS_H