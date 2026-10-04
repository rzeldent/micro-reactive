#pragma once

#include <core.h>
#include <scheduler.h>
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

    // Factory function for Filter operator
    template <typename T, typename Predicate>
    std::shared_ptr<IObservable<T>> Filter(
        std::shared_ptr<IObservable<T>> observable, Predicate predicate)
    {
        return std::make_shared<FilterOperator<T>>(
            observable, std::function<bool(const T &)>(predicate));
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
            std::set<T> seen_;

        public:
            DistinctObserver(Operator<T> *op) : operator_(op) {}

            void OnNext(const T &value) override
            {
                if (seen_.find(value) == seen_.end())
                {
                    seen_.insert(value);
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

    // First operator - emits only the first item
    template <typename T>
    class FirstOperator : public Operator<T>
    {
        class FirstObserver : public IObserver<T>
        {
        private:
            Operator<T> *operator_;
            bool emitted_;

        public:
            FirstObserver(Operator<T> *op) : operator_(op), emitted_(false) {}

            void OnNext(const T &value) override
            {
                if (!emitted_)
                {
                    emitted_ = true;
                    operator_->NotifyOnNext(value);
                    operator_->NotifyOnCompleted();
                }
            }

            void OnCompleted() override
            {
                if (!emitted_)
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

    // =============================================================================
    // HYSTERESIS OPERATOR - Prevents rapid switching around a threshold
    // =============================================================================
    template <typename T>
    class HysteresisOperator : public Operator<bool>
    {
        class HysteresisObserver : public IObserver<T>
        {
        private:
            Operator<bool> *operator_;
            double threshold_;
            double hysteresis_;
            bool state_;
            bool initialized_;

        public:
            HysteresisObserver(Operator<bool> *op, double threshold, double hysteresis)
                : operator_(op), threshold_(threshold), hysteresis_(hysteresis),
                  state_(false), initialized_(false)
            {
            }

            void OnNext(const T &value) override
            {
                double val = static_cast<double>(value);
                double upper = threshold_ + hysteresis_;
                double lower = threshold_ - hysteresis_;

                if (!initialized_)
                {
                    // Initialize state based on first value
                    state_ = val > threshold_;
                    initialized_ = true;
                    operator_->NotifyOnNext(state_);
                }
                else if (state_ && val < lower)
                {
                    // Was ON, now below lower threshold -> turn OFF
                    state_ = false;
                    operator_->NotifyOnNext(state_);
                }
                else if (!state_ && val > upper)
                {
                    // Was OFF, now above upper threshold -> turn ON
                    state_ = true;
                    operator_->NotifyOnNext(state_);
                }
                // If within hysteresis band, keep current state (no output)
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
        std::shared_ptr<HysteresisObserver> observer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex subscription_mutex_;

    public:
        HysteresisOperator(std::shared_ptr<IObservable<T>> observable, double threshold, double hysteresis)
            : observable_(observable)
        {
            observer_ = std::make_shared<HysteresisObserver>(this, threshold, hysteresis);
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

            auto weak_self = std::weak_ptr<HysteresisOperator<T>>(
                std::static_pointer_cast<HysteresisOperator<T>>(this->shared_from_this()));
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

    // Factory function for Hysteresis operator
    template <typename T>
    std::shared_ptr<IObservable<bool>> Hysteresis(std::shared_ptr<IObservable<T>> observable, double threshold, double hysteresis)
    {
        static_assert(std::is_arithmetic<T>::value, "Hysteresis requires an arithmetic sample type");
        if (!observable || !std::isfinite(threshold) || !std::isfinite(hysteresis) || hysteresis < 0.0)
            throw std::invalid_argument("Invalid Hysteresis parameters");
        return std::make_shared<HysteresisOperator<T>>(observable, threshold, hysteresis);
    }

    template <typename T>
    std::shared_ptr<IObservable<bool>> Hysteresis(std::shared_ptr<Subject<T>> source, double threshold, double hysteresis)
    {
        return Hysteresis<T>(
            std::static_pointer_cast<IObservable<T>>(source), threshold, hysteresis);
    }

} // namespace rx
