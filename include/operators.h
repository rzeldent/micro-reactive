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
    std::shared_ptr<MapOperator<Tsrc, Tdest>> Map(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform)
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
    std::shared_ptr<FilterOperator<T>> Filter(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
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
    std::shared_ptr<TakeOperator<T>> Take(std::shared_ptr<IObservable<T>> observable, size_t count)
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
    std::shared_ptr<SkipOperator<T>> Skip(std::shared_ptr<IObservable<T>> observable, size_t count)
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
    std::shared_ptr<DistinctOperator<T>> Distinct(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<DistinctOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<DistinctOperator<T>> Distinct(std::shared_ptr<Subject<T>> subject)
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

    // =============================================================================
    // ALL OPERATOR - Emits true if all items satisfy the predicate
    // =============================================================================
    template <typename T>
    class AllOperator : public Operator<bool>
    {
        class AllObserver : public IObserver<T>
        {
        private:
            Operator<bool> *operator_;
            std::function<bool(const T &)> predicate_;
            bool all_satisfied_;

        public:
            AllObserver(Operator<bool> *op, std::function<bool(const T &)> predicate)
                : operator_(op), predicate_(predicate), all_satisfied_(true)
            {
            }

            void OnNext(const T &value) override
            {
                if (all_satisfied_ && !predicate_(value))
                {
                    all_satisfied_ = false;
                    operator_->NotifyOnNext(false);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnCompleted() override
            {
                if (all_satisfied_)
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

    // =============================================================================
    // ANY OPERATOR - Emits true if any item satisfies the predicate
    // =============================================================================
    template <typename T>
    class AnyOperator : public Operator<bool>
    {
        class AnyObserver : public IObserver<T>
        {
        private:
            Operator<bool> *operator_;
            std::function<bool(const T &)> predicate_;
            bool any_satisfied_;

        public:
            AnyObserver(Operator<bool> *op, std::function<bool(const T &)> predicate)
                : operator_(op), predicate_(predicate), any_satisfied_(false)
            {
            }

            void OnNext(const T &value) override
            {
                if (!any_satisfied_ && predicate_(value))
                {
                    any_satisfied_ = true;
                    operator_->NotifyOnNext(true);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnCompleted() override
            {
                if (!any_satisfied_)
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

    // =============================================================================
    // SCAN OPERATOR - Accumulate over time, emitting intermediate results
    // =============================================================================
    template <typename T, typename TAcc>
    class ScanOperator : public Operator<TAcc>
    {
        class ScanObserver : public IObserver<T>
        {
        private:
            Operator<TAcc> *operator_;
            TAcc accumulator_;
            std::function<TAcc(const TAcc &, const T &)> accumulatorFunc_;
            bool first_;

        public:
            ScanObserver(Operator<TAcc> *op, TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulatorFunc)
                : operator_(op), accumulator_(seed), accumulatorFunc_(accumulatorFunc), first_(true) {}

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

    // =============================================================================
    // REDUCE OPERATOR - Accumulate over time, emitting only the final result
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

    // =============================================================================
    // THROTTLE OPERATOR - Emits items only after a specified time period has passed
    // =============================================================================
    template <typename T>
    class ThrottleOperator : public Operator<T>
    {
        class ThrottleObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            size_t interval_;
            std::atomic<size_t> count_;

        public:
            ThrottleObserver(Operator<T> *op, size_t interval)
                : operator_(op), interval_(interval), count_(0) {}

            void OnNext(const T &value) override
            {
                size_t current_count = count_.fetch_add(1) + 1;
                if (current_count % interval_ == 0)
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

    // =============================================================================
    // FIRST OPERATOR - Emits only the first item
    // =============================================================================
    template <typename T>
    class FirstOperator : public Operator<T>
    {
        class FirstObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::atomic<bool> emitted_;

        public:
            FirstObserver(Operator<T> *op) : operator_(op), emitted_(false) {}

            void OnNext(const T &value) override
            {
                if (!emitted_.exchange(true))
                {
                    operator_->NotifyOnNext(value);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnCompleted() override
            {
                if (!emitted_.load())
                {
                    operator_->NotifyOnError(std::runtime_error("Sequence contains no elements"));
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

    // =============================================================================
    // LAST OPERATOR - Emits only the last item
    // =============================================================================
    template <typename T>
    class LastOperator : public Operator<T>
    {
        class LastObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            T lastValue_;
            bool hasValue_;

        public:
            LastObserver(Operator<T> *op) : operator_(op), hasValue_(false) {}

            void OnNext(const T &value) override
            {
                lastValue_ = value;
                hasValue_ = true;
            }

            void OnCompleted() override
            {
                if (hasValue_)
                {
                    operator_->NotifyOnNext(lastValue_);
                    operator_->NotifyOnCompleted();
                }
                else
                {
                    operator_->NotifyOnError(std::runtime_error("Sequence contains no elements"));
                }
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

    // =============================================================================
    // FACTORY FUNCTIONS
    // =============================================================================

    // Factory functions for Do/Tap
    template <typename T>
    std::shared_ptr<DoOperator<T>> Do(std::shared_ptr<IObservable<T>> observable, std::function<void(const T &)> action)
    {
        return std::make_shared<DoOperator<T>>(observable, action);
    }

    template <typename T>
    std::shared_ptr<DoOperator<T>> Do(std::shared_ptr<Subject<T>> subject, std::function<void(const T &)> action)
    {
        return Do(std::static_pointer_cast<IObservable<T>>(subject), action);
    }

    template <typename T>
    std::shared_ptr<DoOperator<T>> Tap(std::shared_ptr<IObservable<T>> observable, std::function<void(const T &)> action)
    {
        return Do(observable, action);
    }

    template <typename T>
    std::shared_ptr<DoOperator<T>> Tap(std::shared_ptr<Subject<T>> subject, std::function<void(const T &)> action)
    {
        return Do(std::static_pointer_cast<IObservable<T>>(subject), action);
    }

    // Factory functions for Contains
    template <typename T>
    std::shared_ptr<ContainsOperator<T>> Contains(std::shared_ptr<IObservable<T>> observable, const T &value)
    {
        return std::make_shared<ContainsOperator<T>>(observable, value);
    }

    template <typename T>
    std::shared_ptr<ContainsOperator<T>> Contains(std::shared_ptr<Subject<T>> subject, const T &value)
    {
        return Contains(std::static_pointer_cast<IObservable<T>>(subject), value);
    }

    // Factory functions for TakeUntil
    template <typename T, typename TriggerType>
    std::shared_ptr<TakeUntilOperator<T, TriggerType>> TakeUntil(std::shared_ptr<IObservable<T>> source, std::shared_ptr<IObservable<TriggerType>> trigger)
    {
        return std::make_shared<TakeUntilOperator<T, TriggerType>>(source, trigger);
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<TakeUntilOperator<T, TriggerType>> TakeUntil(std::shared_ptr<Subject<T>> source, std::shared_ptr<IObservable<TriggerType>> trigger)
    {
        return TakeUntil(std::static_pointer_cast<IObservable<T>>(source), trigger);
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<TakeUntilOperator<T, TriggerType>> TakeUntil(std::shared_ptr<IObservable<T>> source, std::shared_ptr<Subject<TriggerType>> trigger)
    {
        return TakeUntil(source, std::static_pointer_cast<IObservable<TriggerType>>(trigger));
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<TakeUntilOperator<T, TriggerType>> TakeUntil(std::shared_ptr<Subject<T>> source, std::shared_ptr<Subject<TriggerType>> trigger)
    {
        return TakeUntil(std::static_pointer_cast<IObservable<T>>(source), std::static_pointer_cast<IObservable<TriggerType>>(trigger));
    }

    // Factory functions for SkipUntil
    template <typename T, typename TriggerType>
    std::shared_ptr<SkipUntilOperator<T, TriggerType>> SkipUntil(std::shared_ptr<IObservable<T>> source, std::shared_ptr<IObservable<TriggerType>> trigger)
    {
        return std::make_shared<SkipUntilOperator<T, TriggerType>>(source, trigger);
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<SkipUntilOperator<T, TriggerType>> SkipUntil(std::shared_ptr<Subject<T>> source, std::shared_ptr<IObservable<TriggerType>> trigger)
    {
        return SkipUntil(std::static_pointer_cast<IObservable<T>>(source), trigger);
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<SkipUntilOperator<T, TriggerType>> SkipUntil(std::shared_ptr<IObservable<T>> source, std::shared_ptr<Subject<TriggerType>> trigger)
    {
        return SkipUntil(source, std::static_pointer_cast<IObservable<TriggerType>>(trigger));
    }

    template <typename T, typename TriggerType>
    std::shared_ptr<SkipUntilOperator<T, TriggerType>> SkipUntil(std::shared_ptr<Subject<T>> source, std::shared_ptr<Subject<TriggerType>> trigger)
    {
        return SkipUntil(std::static_pointer_cast<IObservable<T>>(source), std::static_pointer_cast<IObservable<TriggerType>>(trigger));
    }

    // Factory functions for All
    template <typename T>
    std::shared_ptr<AllOperator<T>> All(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
    {
        return std::make_shared<AllOperator<T>>(observable, predicate);
    }

    template <typename T>
    std::shared_ptr<AllOperator<T>> All(std::shared_ptr<Subject<T>> subject, std::function<bool(const T &)> predicate)
    {
        return All(std::static_pointer_cast<IObservable<T>>(subject), predicate);
    }

    // Factory functions for Any
    template <typename T>
    std::shared_ptr<AnyOperator<T>> Any(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
    {
        return std::make_shared<AnyOperator<T>>(observable, predicate);
    }

    template <typename T>
    std::shared_ptr<AnyOperator<T>> Any(std::shared_ptr<Subject<T>> subject, std::function<bool(const T &)> predicate)
    {
        return Any(std::static_pointer_cast<IObservable<T>>(subject), predicate);
    }

    // Factory functions for Scan
    template <typename T, typename TAcc>
    std::shared_ptr<ScanOperator<T, TAcc>> Scan(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulator)
    {
        return std::make_shared<ScanOperator<T, TAcc>>(observable, seed, accumulator);
    }

    // Factory functions for Reduce
    template <typename T, typename TAcc>
    std::shared_ptr<ReduceOperator<T, TAcc>> Reduce(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc &, const T &)> accumulator)
    {
        return std::make_shared<ReduceOperator<T, TAcc>>(observable, seed, accumulator);
    }

    // Factory functions for Throttle
    template <typename T>
    std::shared_ptr<ThrottleOperator<T>> Throttle(std::shared_ptr<IObservable<T>> observable, size_t interval)
    {
        return std::make_shared<ThrottleOperator<T>>(observable, interval);
    }

    // Factory functions for First
    template <typename T>
    std::shared_ptr<FirstOperator<T>> First(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<FirstOperator<T>>(observable);
    }

    // Factory functions for Last
    template <typename T>
    std::shared_ptr<LastOperator<T>> Last(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<LastOperator<T>>(observable);
    }

    // Aliases for commonly used functions
    template <typename T>
    std::shared_ptr<FilterOperator<T>> Where(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
    {
        return Filter(observable, predicate);
    }

    template <typename Tsrc, typename Tdest>
    std::shared_ptr<MapOperator<Tsrc, Tdest>> Select(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform)
    {
        return Map(observable, transform);
    }

    // =============================================================================
    // TAKEWHILE OPERATOR - Takes items while a condition is true
    // =============================================================================
    template <typename T>
    class TakeWhileOperator : public Operator<T>
    {
        class TakeWhileObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::function<bool(const T &)> predicate_;
            std::atomic<bool> completed_{false};

        public:
            TakeWhileObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
                : operator_(op), predicate_(predicate)
            {
            }

            void OnNext(const T &value) override
            {
                if (!completed_.load() && predicate_(value))
                {
                    operator_->NotifyOnNext(value);
                }
                else if (!completed_.exchange(true))
                {
                    operator_->NotifyOnCompleted();
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

    // =============================================================================
    // SKIPWHILE OPERATOR - Skips items while a condition is true
    // =============================================================================
    template <typename T>
    class SkipWhileOperator : public Operator<T>
    {
        class SkipWhileObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::function<bool(const T &)> predicate_;
            std::atomic<bool> skipping_{true};

        public:
            SkipWhileObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
                : operator_(op), predicate_(predicate)
            {
            }

            void OnNext(const T &value) override
            {
                if (skipping_.load() && predicate_(value))
                {
                    // Continue skipping
                    return;
                }
                else
                {
                    skipping_.store(false);
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

    // =============================================================================
    // DISTINCTUNTILCHANGED OPERATOR - Only emit items when they differ from the previous item
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

    // Factory functions for the final three operators
    template <typename T>
    std::shared_ptr<DistinctUntilChangedOperator<T>> DistinctUntilChanged(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<DistinctUntilChangedOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<PairwiseOperator<T>> Pairwise(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<PairwiseOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<RaceOperator<T>> Race(std::vector<std::shared_ptr<IObservable<T>>> observables)
    {
        return std::make_shared<RaceOperator<T>>(observables);
    }

    // Missing factory functions for TakeWhile and SkipWhile
    template <typename T>
    std::shared_ptr<TakeWhileOperator<T>> TakeWhile(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
    {
        return std::make_shared<TakeWhileOperator<T>>(observable, predicate);
    }

    template <typename T>
    std::shared_ptr<SkipWhileOperator<T>> SkipWhile(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
    {
        return std::make_shared<SkipWhileOperator<T>>(observable, predicate);
    }

    // =============================================================================
    // COUNT OPERATOR - Counts the number of items emitted by the source
    // =============================================================================
    template <typename T>
    class CountOperator : public Operator<size_t>
    {
        class CountObserver : public IObserver<T>
        {
        private:
            Operator<size_t> *operator_;
            std::atomic<size_t> count_;

        public:
            CountObserver(Operator<size_t> *op) : operator_(op), count_(0) {}

            void OnNext(const T &value) override
            {
                count_.fetch_add(1);
            }

            void OnCompleted() override
            {
                operator_->NotifyOnNext(count_.load());
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
                        self->source_subscription_ = nullptr;
                    }
                } });
        }
    };

    // =============================================================================
    // SUM OPERATOR - Sums all numeric values emitted by the source
    // =============================================================================
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
                        self->source_subscription_ = nullptr;
                    }
                } });
        }
    };

    // =============================================================================
    // AVERAGE OPERATOR - Computes the average of numeric values
    // =============================================================================
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
                    operator_->NotifyOnNext(sum_ / static_cast<T>(count_));
                }
                else
                {
                    operator_->NotifyOnNext(T{});
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
                        self->source_subscription_ = nullptr;
                    }
                } });
        }
    };

    // =============================================================================
    // MIN OPERATOR - Finds the minimum value
    // =============================================================================
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
                        self->source_subscription_ = nullptr;
                    }
                } });
        }
    };

    // =============================================================================
    // MAX OPERATOR - Finds the maximum value
    // =============================================================================
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
                        self->source_subscription_ = nullptr;
                    }
                } });
        }
    };

    // =============================================================================
    // DEFAULTIFEMPTY OPERATOR - Emits a default value if the source is empty
    // =============================================================================
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
            DefaultIfEmptyObserver(Operator<T> *op, T default_value) 
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
        DefaultIfEmptyOperator(std::shared_ptr<IObservable<T>> observable, T default_value) : observable_(observable)
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
                        self->source_subscription_ = nullptr;
                    }
                } });
        }
    };

    // =============================================================================
    // STARTWITH OPERATOR - Emits specified values before the source observable
    // =============================================================================
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
        StartWithOperator(std::shared_ptr<IObservable<T>> observable, std::vector<T> start_values) 
            : observable_(observable), start_values_(start_values)
        {
            observer_ = std::make_shared<StartWithObserver>(this);
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            // First emit the start values
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
                        self->source_subscription_ = nullptr;
                    }
                } });
        }
    };

    // =============================================================================
    // CONCAT OPERATOR - Emits values from first observable, then second when first completes
    // =============================================================================
    template <typename T>
    class ConcatOperator : public Operator<T>
    {
        class FirstObserver : public IObserver<T>
        {
        private:
            ConcatOperator<T> *operator_;

        public:
            FirstObserver(ConcatOperator<T> *op) : operator_(op) {}

            void OnNext(const T &value) override
            {
                operator_->NotifyOnNext(value);
            }

            void OnCompleted() override
            {
                operator_->StartSecondObservable();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        class SecondObserver : public IObserver<T>
        {
        private:
            ConcatOperator<T> *operator_;

        public:
            SecondObserver(ConcatOperator<T> *op) : operator_(op) {}

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

        std::shared_ptr<IObservable<T>> first_observable_;
        std::shared_ptr<IObservable<T>> second_observable_;
        std::shared_ptr<FirstObserver> first_observer_;
        std::shared_ptr<SecondObserver> second_observer_;
        std::shared_ptr<Subscription> first_subscription_;
        std::shared_ptr<Subscription> second_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        ConcatOperator(std::shared_ptr<IObservable<T>> first_observable, std::shared_ptr<IObservable<T>> second_observable) 
            : first_observable_(first_observable), second_observable_(second_observable)
        {
            first_observer_ = std::make_shared<FirstObserver>(this);
            second_observer_ = std::make_shared<SecondObserver>(this);
        }

        void StartSecondObservable()
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (!second_subscription_)
            {
                second_subscription_ = second_observable_->Subscribe(second_observer_);
            }
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !first_subscription_)
                {
                    first_subscription_ = first_observable_->Subscribe(first_observer_);
                }
            }

            auto weak_self = std::weak_ptr<ConcatOperator<T>>(
                std::static_pointer_cast<ConcatOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty()) {
                        if (self->first_subscription_) {
                            self->first_subscription_->Dispose();
                            self->first_subscription_ = nullptr;
                        }
                        if (self->second_subscription_) {
                            self->second_subscription_->Dispose();
                            self->second_subscription_ = nullptr;
                        }
                    }
                } });
        }
    };

    // =============================================================================
    // ZIP OPERATOR - Combines values from two observables using a selector function
    // =============================================================================
    template <typename T1, typename T2, typename TResult>
    class ZipOperator : public Operator<TResult>
    {
        class FirstObserver : public IObserver<T1>
        {
        private:
            ZipOperator<T1, T2, TResult> *operator_;

        public:
            FirstObserver(ZipOperator<T1, T2, TResult> *op) : operator_(op) {}

            void OnNext(const T1 &value) override
            {
                operator_->OnFirstValue(value);
            }

            void OnCompleted() override
            {
                operator_->OnFirstCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        class SecondObserver : public IObserver<T2>
        {
        private:
            ZipOperator<T1, T2, TResult> *operator_;

        public:
            SecondObserver(ZipOperator<T1, T2, TResult> *op) : operator_(op) {}

            void OnNext(const T2 &value) override
            {
                operator_->OnSecondValue(value);
            }

            void OnCompleted() override
            {
                operator_->OnSecondCompleted();
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T1>> first_observable_;
        std::shared_ptr<IObservable<T2>> second_observable_;
        std::function<TResult(const T1&, const T2&)> selector_;
        std::shared_ptr<FirstObserver> first_observer_;
        std::shared_ptr<SecondObserver> second_observer_;
        std::shared_ptr<Subscription> first_subscription_;
        std::shared_ptr<Subscription> second_subscription_;
        
        std::queue<T1> first_values_;
        std::queue<T2> second_values_;
        bool first_completed_;
        bool second_completed_;
        mutable std::mutex subscription_mutex_;

    public:
        ZipOperator(std::shared_ptr<IObservable<T1>> first_observable, 
                   std::shared_ptr<IObservable<T2>> second_observable,
                   std::function<TResult(const T1&, const T2&)> selector) 
            : first_observable_(first_observable), second_observable_(second_observable), 
              selector_(selector), first_completed_(false), second_completed_(false)
        {
            first_observer_ = std::make_shared<FirstObserver>(this);
            second_observer_ = std::make_shared<SecondObserver>(this);
        }

        void OnFirstValue(const T1& value)
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            first_values_.push(value);
            EmitIfPossible();
        }

        void OnSecondValue(const T2& value)
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            second_values_.push(value);
            EmitIfPossible();
        }

        void OnFirstCompleted()
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            first_completed_ = true;
            CheckCompletion();
        }

        void OnSecondCompleted()
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            second_completed_ = true;
            CheckCompletion();
        }

        void CheckCompletion()
        {
            // Complete when both sources are completed OR when one is completed and its queue is empty
            if ((first_completed_ && second_completed_) || 
                (first_completed_ && first_values_.empty()) ||
                (second_completed_ && second_values_.empty()))
            {
                this->NotifyOnCompleted();
            }
        }

        void EmitIfPossible()
        {
            while (!first_values_.empty() && !second_values_.empty())
            {
                T1 first_value = first_values_.front();
                T2 second_value = second_values_.front();
                first_values_.pop();
                second_values_.pop();
                
                TResult result = selector_(first_value, second_value);
                this->NotifyOnNext(result);
            }
            
            // Check if we should complete after emitting
            CheckCompletion();
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<TResult>> observer) override
        {
            auto subscription = Operator<TResult>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !first_subscription_)
                {
                    first_subscription_ = first_observable_->Subscribe(first_observer_);
                    second_subscription_ = second_observable_->Subscribe(second_observer_);
                }
            }

            auto weak_self = std::weak_ptr<ZipOperator<T1, T2, TResult>>(
                std::static_pointer_cast<ZipOperator<T1, T2, TResult>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty()) {
                        if (self->first_subscription_) {
                            self->first_subscription_->Dispose();
                            self->first_subscription_ = nullptr;
                        }
                        if (self->second_subscription_) {
                            self->second_subscription_->Dispose();
                            self->second_subscription_ = nullptr;
                        }
                    }
                } });
        }
    };

    // =============================================================================
    // MERGE OPERATOR - Merges multiple observables into one
    // =============================================================================
    template <typename T>
    class MergeOperator : public Operator<T>
    {
        class MergeObserver : public IObserver<T>
        {
        private:
            MergeOperator<T> *operator_;
            size_t observer_index_;

        public:
            MergeObserver(MergeOperator<T> *op, size_t index) : operator_(op), observer_index_(index) {}

            void OnNext(const T &value) override
            {
                operator_->NotifyOnNext(value);
            }

            void OnCompleted() override
            {
                operator_->OnObserverCompleted(observer_index_);
            }

            void OnError(const std::exception &e) override
            {
                operator_->NotifyOnError(e);
            }
        };

        std::vector<std::shared_ptr<IObservable<T>>> observables_;
        std::vector<std::shared_ptr<MergeObserver>> observers_;
        std::vector<std::shared_ptr<Subscription>> source_subscriptions_;
        std::vector<bool> completed_;
        mutable std::mutex subscription_mutex_;

    public:
        MergeOperator(std::vector<std::shared_ptr<IObservable<T>>> observables) 
            : observables_(observables), completed_(observables.size(), false)
        {
            for (size_t i = 0; i < observables.size(); ++i)
            {
                observers_.push_back(std::make_shared<MergeObserver>(this, i));
            }
        }

        void OnObserverCompleted(size_t index)
        {
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            completed_[index] = true;
            
            // Check if all observers have completed
            bool all_completed = true;
            for (bool completed : completed_)
            {
                if (!completed)
                {
                    all_completed = false;
                    break;
                }
            }
            
            if (all_completed)
            {
                this->NotifyOnCompleted();
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

            auto weak_self = std::weak_ptr<MergeOperator<T>>(
                std::static_pointer_cast<MergeOperator<T>>(this->shared_from_this()));
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
    };

    // =============================================================================
    // DEBOUNCE OPERATOR - Emits items only after a specified time period has passed without another emission
    // =============================================================================
    template <typename T>
    class DebounceOperator : public Operator<T>
    {
        class DebounceObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::chrono::milliseconds timeout_;
            std::shared_ptr<IScheduler> scheduler_;
            T last_value_;
            bool has_value_;
            std::shared_ptr<IScheduledWork> current_work_;
            mutable std::mutex debounce_mutex_;

        public:
            DebounceObserver(Operator<T> *op, std::chrono::milliseconds timeout, std::shared_ptr<IScheduler> scheduler)
                : operator_(op), timeout_(timeout), scheduler_(scheduler), has_value_(false) {}

            void OnNext(const T &value) override
            {
                std::lock_guard<std::mutex> lock(debounce_mutex_);
                
                // Cancel previous timer if any
                if (current_work_)
                {
                    current_work_->Cancel();
                }
                
                // Store the new value
                last_value_ = value;
                has_value_ = true;
                
                // Schedule emission after timeout
                current_work_ = scheduler_->ScheduleDelayed([this]() {
                    std::lock_guard<std::mutex> inner_lock(debounce_mutex_);
                    if (has_value_)
                    {
                        operator_->NotifyOnNext(last_value_);
                        has_value_ = false;
                    }
                }, timeout_);
            }

            void OnCompleted() override
            {
                std::lock_guard<std::mutex> lock(debounce_mutex_);
                
                // Cancel pending timer
                if (current_work_)
                {
                    current_work_->Cancel();
                }
                
                // Emit final value if any
                if (has_value_)
                {
                    operator_->NotifyOnNext(last_value_);
                    has_value_ = false;
                }
                
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                std::lock_guard<std::mutex> lock(debounce_mutex_);
                
                // Cancel pending timer
                if (current_work_)
                {
                    current_work_->Cancel();
                }
                
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<DebounceObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        DebounceOperator(std::shared_ptr<IObservable<T>> observable, std::chrono::milliseconds timeout, std::shared_ptr<IScheduler> scheduler)
            : observable_(observable)
        {
            observer_ = std::make_shared<DebounceObserver>(this, timeout, scheduler);
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

            auto weak_self = std::weak_ptr<DebounceOperator<T>>(
                std::static_pointer_cast<DebounceOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_ = nullptr;
                    }
                } });
        }
    };

    // =============================================================================
    // DEBUG OPERATOR - Observable debugging with metrics
    // =============================================================================
    template <typename T>
    class DebugOperator : public Operator<T>
    {
        class DebugObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::string name_;
            std::shared_ptr<ObservableMetrics> metrics_;

        public:
            DebugObserver(Operator<T> *op, const std::string& name, std::shared_ptr<ObservableMetrics> metrics)
                : operator_(op), name_(name), metrics_(metrics) {}

            void OnNext(const T &value) override
            {
                if (metrics_)
                {
                    metrics_->RecordEmission();
                }
                
                #ifdef MICRO_REACTIVE_DEBUG
                // Could add Serial.print for ESP32 debugging
                // For now, just pass through
                #endif
                
                operator_->NotifyOnNext(value);
            }

            void OnCompleted() override
            {
                if (metrics_)
                {
                    metrics_->RecordCompletion();
                }
                
                #ifdef MICRO_REACTIVE_DEBUG
                // Could add completion logging
                #endif
                
                operator_->NotifyOnCompleted();
            }

            void OnError(const std::exception &e) override
            {
                if (metrics_)
                {
                    metrics_->RecordError();
                }
                
                #ifdef MICRO_REACTIVE_DEBUG
                // Could add error logging
                #endif
                
                operator_->NotifyOnError(e);
            }
        };

        std::shared_ptr<IObservable<T>> observable_;
        std::shared_ptr<DebugObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        std::shared_ptr<ObservableMetrics> metrics_;
        std::string name_;
        mutable std::mutex subscription_mutex_;

    public:
        DebugOperator(std::shared_ptr<IObservable<T>> observable, const std::string& name)
            : observable_(observable), name_(name), metrics_(std::make_shared<ObservableMetrics>())
        {
            observer_ = std::make_shared<DebugObserver>(this, name, metrics_);
        }

        std::shared_ptr<ObservableMetrics> GetMetrics() const
        {
            return metrics_;
        }

        std::string GetName() const
        {
            return name_;
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            if (metrics_)
            {
                metrics_->RecordSubscription();
            }
            
            auto subscription = Operator<T>::Subscribe(observer);

            {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                if (this->child_observers_.size() == 1 && !source_subscription_)
                {
                    source_subscription_ = observable_->Subscribe(observer_);
                }
            }

            auto weak_self = std::weak_ptr<DebugOperator<T>>(
                std::static_pointer_cast<DebugOperator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, subscription, observer]()
                                                  {
                if (auto self = weak_self.lock()) {
                    subscription->Dispose();
                    std::lock_guard<std::mutex> lock(self->subscription_mutex_);
                    if (self->child_observers_.empty() && self->source_subscription_) {
                        self->source_subscription_->Dispose();
                        self->source_subscription_ = nullptr;
                    }
                } });
        }
    };

    // Factory functions for new operators
    template <typename T>
    std::shared_ptr<CountOperator<T>> Count(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<CountOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<SumOperator<T>> Sum(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<SumOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<AverageOperator<T>> Average(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<AverageOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<MinOperator<T>> Min(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<MinOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<MaxOperator<T>> Max(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<MaxOperator<T>>(observable);
    }

    template <typename T>
    std::shared_ptr<DefaultIfEmptyOperator<T>> DefaultIfEmpty(std::shared_ptr<IObservable<T>> observable, T default_value)
    {
        return std::make_shared<DefaultIfEmptyOperator<T>>(observable, default_value);
    }

    template <typename T>
    std::shared_ptr<StartWithOperator<T>> StartWith(std::shared_ptr<IObservable<T>> observable, std::vector<T> start_values)
    {
        return std::make_shared<StartWithOperator<T>>(observable, start_values);
    }

    template <typename T>
    std::shared_ptr<ConcatOperator<T>> Concat(std::shared_ptr<IObservable<T>> first_observable, std::shared_ptr<IObservable<T>> second_observable)
    {
        return std::make_shared<ConcatOperator<T>>(first_observable, second_observable);
    }

    template <typename T1, typename T2, typename TResult>
    std::shared_ptr<ZipOperator<T1, T2, TResult>> Zip(std::shared_ptr<IObservable<T1>> first_observable,
                                                       std::shared_ptr<IObservable<T2>> second_observable,
                                                       std::function<TResult(const T1&, const T2&)> selector)
    {
        return std::make_shared<ZipOperator<T1, T2, TResult>>(first_observable, second_observable, selector);
    }

    // Specialized Zip for creating pairs
    template <typename T1, typename T2>
    std::shared_ptr<ZipOperator<T1, T2, std::pair<T1, T2>>> Zip(std::shared_ptr<IObservable<T1>> first_observable,
                                                                 std::shared_ptr<IObservable<T2>> second_observable)
    {
        return Zip<T1, T2, std::pair<T1, T2>>(first_observable, second_observable,
                                               [](const T1& a, const T2& b) { return std::make_pair(a, b); });
    }

    template <typename T>
    std::shared_ptr<MergeOperator<T>> Merge(std::vector<std::shared_ptr<IObservable<T>>> observables)
    {
        return std::make_shared<MergeOperator<T>>(observables);
    }

    // Convenience function for merging two observables
    template <typename T>
    std::shared_ptr<MergeOperator<T>> Merge(std::shared_ptr<IObservable<T>> first, std::shared_ptr<IObservable<T>> second)
    {
        std::vector<std::shared_ptr<IObservable<T>>> observables = {first, second};
        return std::make_shared<MergeOperator<T>>(observables);
    }

    // Factory function for Debug operator
    template <typename T>
    std::shared_ptr<DebugOperator<T>> Debug(std::shared_ptr<IObservable<T>> observable, const std::string& name)
    {
        return std::make_shared<DebugOperator<T>>(observable, name);
    }

    // Factory function for Debounce operator
    template <typename T>
    std::shared_ptr<DebounceOperator<T>> Debounce(std::shared_ptr<IObservable<T>> observable, 
                                                  std::chrono::milliseconds timeout, 
                                                  std::shared_ptr<IScheduler> scheduler = nullptr)
    {
        if (!scheduler)
        {
            scheduler = std::make_shared<ThreadPoolScheduler>();
        }
        return std::make_shared<DebounceOperator<T>>(observable, timeout, scheduler);
    }

} // namespace rx

#endif // MICRO_REACTIVE_OPERATORS_H
