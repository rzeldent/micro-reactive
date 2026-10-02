#ifndef MICRO_REACTIVE_OPERATORS_TRANSFORMATION_H
#define MICRO_REACTIVE_OPERATORS_TRANSFORMATION_H

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

    // Factory function for Map operator
    template <typename Tsrc, typename Tdest>
    std::shared_ptr<IObservable<Tdest>> Map(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform)
    {
        return std::make_shared<MapOperator<Tsrc, Tdest>>(observable, transform);
    }

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

} // namespace rx

#endif // MICRO_REACTIVE_OPERATORS_TRANSFORMATION_H
