#ifndef MICRO_REACTIVE_OPERATORS_AGGREGATION_H
#define MICRO_REACTIVE_OPERATORS_AGGREGATION_H

#include "../core.h"
#include "../scheduler.h"
#include "../performance.h"
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

    // All operator - checks if all items satisfy predicate
    template <typename T>
    class AllOperator : public Operator<bool>
    {
        class AllObserver : public IObserver<T>
        {
        private:
            Operator<bool> *operator_;
            std::function<bool(const T &)> predicate_;
            bool completed_;

        public:
            AllObserver(Operator<bool> *op, std::function<bool(const T &)> predicate)
                : operator_(op), predicate_(predicate), completed_(false) {}

            void OnNext(const T &value) override
            {
                if (!completed_ && !predicate_(value))
                {
                    completed_ = true;
                    operator_->NotifyOnNext(false);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnCompleted() override
            {
                if (!completed_)
                {
                    completed_ = true;
                    operator_->NotifyOnNext(true);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnError(const std::exception &e) override
            {
                if (!completed_)
                {
                    completed_ = true;
                    operator_->NotifyOnError(e);
                }
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
            bool completed_;

        public:
            AnyObserver(Operator<bool> *op, std::function<bool(const T &)> predicate)
                : operator_(op), predicate_(predicate), completed_(false) {}

            void OnNext(const T &value) override
            {
                if (!completed_ && predicate_(value))
                {
                    completed_ = true;
                    operator_->NotifyOnNext(true);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnCompleted() override
            {
                if (!completed_)
                {
                    completed_ = true;
                    operator_->NotifyOnNext(false);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnError(const std::exception &e) override
            {
                if (!completed_)
                {
                    completed_ = true;
                    operator_->NotifyOnError(e);
                }
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

            void OnNext(const T &) override
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

    template <typename T>
    std::shared_ptr<CountOperator<T>> Count(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<CountOperator<T>>(observable);
    }

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

    template <typename T>
    std::shared_ptr<SumOperator<T>> Sum(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<SumOperator<T>>(observable);
    }

    // Average operator - calculates average of all emitted numeric values
    template <typename T>
    class AverageOperator : public Operator<double>
    {
        class AverageObserver : public IObserver<T>
        {
        private:
            Operator<double> *operator_;
            T sum_;
            size_t count_;

        public:
            AverageObserver(Operator<double> *op) : operator_(op), sum_(T{}), count_(0) {}

            void OnNext(const T &value) override
            {
                sum_ += value;
                count_++;
            }

            void OnCompleted() override
            {
                if (count_ > 0)
                {
                    operator_->NotifyOnNext(static_cast<double>(sum_) / count_);
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

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<double>> observer) override
        {
            auto subscription = Operator<double>::Subscribe(observer);

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

        void UnSubscribe(std::shared_ptr<IObserver<double>> observer) override
        {
            Operator<double>::UnSubscribe(observer);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            if (this->child_observers_.empty() && source_subscription_)
            {
                source_subscription_->Dispose();
                source_subscription_.reset();
            }
        }
    };

    template <typename T>
    std::shared_ptr<AverageOperator<T>> Average(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<AverageOperator<T>>(observable);
    }

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

    template <typename T>
    std::shared_ptr<MinOperator<T>> Min(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<MinOperator<T>>(observable);
    }

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

    template <typename T>
    std::shared_ptr<MaxOperator<T>> Max(std::shared_ptr<IObservable<T>> observable)
    {
        return std::make_shared<MaxOperator<T>>(observable);
    }

} // namespace rx

#endif // MICRO_REACTIVE_OPERATORS_AGGREGATION_H
