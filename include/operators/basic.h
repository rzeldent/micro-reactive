#pragma once

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

} // namespace rx
