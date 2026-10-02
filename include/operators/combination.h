#ifndef MICRO_REACTIVE_OPERATORS_COMBINATION_H
#define MICRO_REACTIVE_OPERATORS_COMBINATION_H

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
                : operator_(op), first_emitted_(first_emitted), is_winner_(is_winner) {}

            void OnNext(const T &value) override
            {
                bool expected = false;
                if (first_emitted_->compare_exchange_strong(expected, true))
                {
                    is_winner_->store(true);
                    operator_->NotifyOnNext(value);
                }
                else if (is_winner_->load())
                {
                    operator_->NotifyOnNext(value);
                }
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
        std::atomic<bool> first_emitted_;
        std::vector<std::atomic<bool>> is_winner_;
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

} // namespace rx

#endif // MICRO_REACTIVE_OPERATORS_COMBINATION_H
