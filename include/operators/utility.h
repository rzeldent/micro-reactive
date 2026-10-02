#ifndef MICRO_REACTIVE_OPERATORS_UTILITY_H
#define MICRO_REACTIVE_OPERATORS_UTILITY_H

#include "../core.h"
#include "../scheduler.h"
#include <functional>
#include <memory>
#include <vector>
#include <cstddef>
#include <chrono>
#include <queue>
#include <set>
#include <atomic>
#include <string>
#include <map>

namespace rx
{
    // =============================================================================
    // DO OPERATOR - Performs side effects without modifying the stream
    // =============================================================================
    template <typename T>
    class DoOperator : public Operator<T>
    {
        class DoObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            std::function<void(const T &)> side_effect_;

        public:
            DoObserver(Operator<T> *op, std::function<void(const T &)> side_effect)
                : operator_(op), side_effect_(side_effect) {}

            void OnNext(const T &value) override
            {
                side_effect_(value);
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
        std::shared_ptr<DoObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        DoOperator(std::shared_ptr<IObservable<T>> observable, std::function<void(const T &)> side_effect) : observable_(observable)
        {
            observer_ = std::make_shared<DoObserver>(this, side_effect);
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
    std::shared_ptr<IObservable<T>> Do(std::shared_ptr<IObservable<T>> observable, std::function<void(const T &)> side_effect)
    {
        return std::make_shared<DoOperator<T>>(observable, side_effect);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Do(std::shared_ptr<Subject<T>> subject, std::function<void(const T &)> side_effect)
    {
        return Do(std::static_pointer_cast<IObservable<T>>(subject), side_effect);
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
            T value_to_find_;
            bool found_;

        public:
            ContainsObserver(Operator<bool> *op, const T &value) : operator_(op), value_to_find_(value), found_(false) {}

            void OnNext(const T &value) override
            {
                if (!found_ && value == value_to_find_)
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
        ContainsOperator(std::shared_ptr<IObservable<T>> observable, const T &value) : observable_(observable)
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
    std::shared_ptr<IObservable<bool>> Contains(std::shared_ptr<IObservable<T>> observable, const T &value)
    {
        return std::make_shared<ContainsOperator<T>>(observable, value);
    }

    template <typename T>
    std::shared_ptr<IObservable<bool>> Contains(std::shared_ptr<Subject<T>> subject, const T &value)
    {
        return Contains(std::static_pointer_cast<IObservable<T>>(subject), value);
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

    // =============================================================================
    // TAKE UNTIL OPERATOR - Takes items until a trigger observable emits
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
    // SKIP UNTIL OPERATOR - Skips items until a trigger observable emits
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
            T previous_value_;
            bool has_previous_;

        public:
            DistinctUntilChangedObserver(Operator<T> *op) : operator_(op), has_previous_(false) {}

            void OnNext(const T &value) override
            {
                if (!has_previous_ || value != previous_value_)
                {
                    operator_->NotifyOnNext(value);
                    previous_value_ = value;
                    has_previous_ = true;
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
            T previous_value_;
            bool has_previous_;

        public:
            PairwiseObserver(Operator<std::pair<T, T>> *op) : operator_(op), has_previous_(false) {}

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

} // namespace rx

#endif // MICRO_REACTIVE_OPERATORS_UTILITY_H
