#pragma once

#include "core.h"
#include <memory>
#include <list>
#include <mutex>
#include <cstddef>

namespace rx
{
    // =============================================================================
    // SUBJECT - Basic subject for multicasting with thread safety
    // =============================================================================
    template <typename T>
    class Subject : public ISubject<T>, public std::enable_shared_from_this<Subject<T>>
    {
    private:
        std::list<std::shared_ptr<IObserver<T>>> child_observers_;
        mutable std::mutex observers_mutex_;

    public:
        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            {
                std::lock_guard<std::mutex> lock(observers_mutex_);
                child_observers_.push_back(observer);
            }

            // Return subscription for cleanup
            auto weak_self = std::weak_ptr<Subject<T>>(std::static_pointer_cast<Subject<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                self->UnSubscribe(observer);
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            std::lock_guard<std::mutex> lock(observers_mutex_);
            child_observers_.remove(observer);
        }

        void OnNext(const T &value) override
        {
            std::lock_guard<std::mutex> lock(observers_mutex_);
            for (auto observer : child_observers_)
            {
                if (observer)
                {
                    observer->OnNext(value);
                }
            }
        }

        void OnCompleted() override
        {
            std::lock_guard<std::mutex> lock(observers_mutex_);
            for (auto observer : child_observers_)
            {
                if (observer)
                {
                    observer->OnCompleted();
                }
            }
        }

        void OnError(const std::exception &e) override
        {
            std::lock_guard<std::mutex> lock(observers_mutex_);
            for (auto observer : child_observers_)
            {
                if (observer)
                {
                    observer->OnError(e);
                }
            }
        }

        // Convert to IObservable for use with operators
        std::shared_ptr<IObservable<T>> AsObservable()
        {
            return std::static_pointer_cast<IObservable<T>>(this->shared_from_this());
        }

        ~Subject() = default;
    };

    template <typename T>
    std::shared_ptr<Subject<T>> CreateSubject()
    {
        return std::make_shared<Subject<T>>();
    }

    // =============================================================================
    // BEHAVIOR SUBJECT - Maintains current value and emits to new subscribers with thread safety
    // =============================================================================
    template <typename T>
    class BehaviorSubject : public ISubject<T>, public std::enable_shared_from_this<BehaviorSubject<T>>
    {
    private:
        std::list<std::shared_ptr<IObserver<T>>> child_observers_;
        T value_;
        bool has_value_;
        mutable std::mutex mutex_;

    public:
        BehaviorSubject(const T &value) : value_(value), has_value_(true)
        {
        }

        BehaviorSubject() : has_value_(false)
        {
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                child_observers_.push_back(observer);
                if (has_value_)
                    observer->OnNext(value_);
            }

            // Return subscription for cleanup
            auto weak_self = std::weak_ptr<BehaviorSubject<T>>(std::static_pointer_cast<BehaviorSubject<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                self->UnSubscribe(observer);
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            std::lock_guard<std::mutex> lock(mutex_);
            child_observers_.remove(observer);
        }

        void OnNext(const T &value) override
        {
            std::lock_guard<std::mutex> lock(mutex_);
            value_ = value;
            has_value_ = true;
            for (auto observer : child_observers_)
            {
                if (observer)
                {
                    observer->OnNext(value);
                }
            }
        }

        void OnCompleted() override
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto observer : child_observers_)
            {
                if (observer)
                {
                    observer->OnCompleted();
                }
            }
        }

        void OnError(const std::exception &e) override
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto observer : child_observers_)
            {
                if (observer)
                {
                    observer->OnError(e);
                }
            }
        }

        T GetValue() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return value_;
        }

        bool HasValue() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return has_value_;
        }

        // Convert to IObservable for use with operators
        std::shared_ptr<IObservable<T>> AsObservable()
        {
            return std::static_pointer_cast<IObservable<T>>(this->shared_from_this());
        }

        ~BehaviorSubject() = default;
    };

    template <typename T>
    std::shared_ptr<BehaviorSubject<T>> CreateBehaviorSubject(const T &value)
    {
        return std::make_shared<BehaviorSubject<T>>(value);
    }

    template <typename T>
    std::shared_ptr<BehaviorSubject<T>> CreateBehaviorSubject()
    {
        return std::make_shared<BehaviorSubject<T>>();
    }

    // =============================================================================
    // REPLAY SUBJECT - Replays a subset of previously emitted items to new subscribers with thread safety
    // =============================================================================
    template <typename T>
    class ReplaySubject : public ISubject<T>, public std::enable_shared_from_this<ReplaySubject<T>>
    {
    private:
        std::list<std::shared_ptr<IObserver<T>>> child_observers_;
        size_t size_;
        std::list<T> values_;
        mutable std::mutex mutex_;

    public:
        ReplaySubject(size_t size) : size_(size)
        {
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                child_observers_.push_back(observer);
                for (const auto &value : values_)
                    observer->OnNext(value);
            }

            // Return subscription for cleanup
            auto weak_self = std::weak_ptr<ReplaySubject<T>>(std::static_pointer_cast<ReplaySubject<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                self->UnSubscribe(observer);
            } });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            std::lock_guard<std::mutex> lock(mutex_);
            child_observers_.remove(observer);
        }

        void OnNext(const T &value) override
        {
            std::lock_guard<std::mutex> lock(mutex_);
            values_.push_back(value);
            if (values_.size() > size_)
                values_.pop_front();

            for (auto observer : child_observers_)
            {
                if (observer)
                {
                    observer->OnNext(value);
                }
            }
        }

        void OnCompleted() override
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto observer : child_observers_)
            {
                if (observer)
                {
                    observer->OnCompleted();
                }
            }
        }

        void OnError(const std::exception &e) override
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto observer : child_observers_)
            {
                if (observer)
                {
                    observer->OnError(e);
                }
            }
        }

        // Convert to IObservable for use with operators
        std::shared_ptr<IObservable<T>> AsObservable()
        {
            return std::static_pointer_cast<IObservable<T>>(this->shared_from_this());
        }

        ~ReplaySubject() = default;
    };

    template <typename T>
    std::shared_ptr<ReplaySubject<T>> CreateReplaySubject(size_t size = 10)
    {
        return std::make_shared<ReplaySubject<T>>(size);
    }

    // =============================================================================
    // SYNCHRONIZED SUBJECT - Thread-safe subject (deprecated - all subjects now thread-safe)
    // =============================================================================
    template <typename T>
    class SynchronizedSubject : public ISubject<T>, public std::enable_shared_from_this<SynchronizedSubject<T>>
    {
    private:
        std::shared_ptr<Subject<T>> subject_;

    public:
        SynchronizedSubject() : subject_(CreateSubject<T>())
        {
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            return subject_->Subscribe(observer);
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            subject_->UnSubscribe(observer);
        }

        void OnNext(const T &value) override
        {
            subject_->OnNext(value);
        }

        void OnCompleted() override
        {
            subject_->OnCompleted();
        }

        void OnError(const std::exception &e) override
        {
            subject_->OnError(e);
        }

        // Convert to IObservable for use with operators
        std::shared_ptr<IObservable<T>> AsObservable()
        {
            return std::static_pointer_cast<IObservable<T>>(this->shared_from_this());
        }

        ~SynchronizedSubject() = default;
    };

    template <typename T>
    std::shared_ptr<SynchronizedSubject<T>> CreateSynchronizedSubject()
    {
        return std::make_shared<SynchronizedSubject<T>>();
    }

} // namespace rx
