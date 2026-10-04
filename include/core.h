#pragma once

#include <memory>
#include <list>
#include <exception>
#include <mutex>
#include <atomic>
#include <functional>

namespace rx
{
    // Forward declaration for disposable pattern
    class IDisposable
    {
    public:
        virtual void Dispose() = 0;
        virtual bool IsDisposed() const = 0;
        virtual ~IDisposable() = default;
    };

    // Subscription token for managing subscriptions
    class Subscription : public IDisposable
    {
    private:
        std::atomic<bool> is_disposed_;
        std::function<void()> dispose_action_;
        mutable std::mutex mutex_;

    public:
        // Default constructor for cases where no dispose action is needed
        Subscription() : is_disposed_(false), dispose_action_(nullptr)
        {
        }

        Subscription(std::function<void()> dispose_action)
            : is_disposed_(false), dispose_action_(std::move(dispose_action))
        {
        }

        void Dispose() override
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!is_disposed_.exchange(true) && dispose_action_)
            {
                dispose_action_();
                dispose_action_ = nullptr;
            }
        }

        bool IsDisposed() const override
        {
            return is_disposed_.load();
        }

        ~Subscription()
        {
            if (!IsDisposed())
            {
                Dispose();
            }
        }
    };

    // Core Observer Interface - Consumes values from an observable
    template <typename T>
    class IObserver
    {
    public:
        virtual void OnNext(const T &value) = 0;
        virtual void OnCompleted() = 0;
        virtual void OnError(const std::exception &e) = 0;

    protected:
        virtual ~IObserver() = default;
    };

    template <typename T>
    class CallbackObserver : public IObserver<T>
    {
    private:
        std::function<void(const T &)> on_next_;
        std::function<void()> on_completed_;
        std::function<void(const std::exception &)> on_error_;

    public:
        CallbackObserver(
            std::function<void(const T &)> on_next,
            std::function<void()> on_completed,
            std::function<void(const std::exception &)> on_error)
            : on_next_(std::move(on_next)),
              on_completed_(std::move(on_completed)),
              on_error_(std::move(on_error))
        {
        }

        void OnNext(const T &value) override
        {
            if (on_next_)
                on_next_(value);
        }

        void OnCompleted() override
        {
            if (on_completed_)
                on_completed_();
        }

        void OnError(const std::exception &error) override
        {
            if (on_error_)
                on_error_(error);
        }
    };

    template <typename T>
    std::shared_ptr<IObserver<T>> CreateObserver(
        std::function<void(const T &)> on_next,
        std::function<void()> on_completed = nullptr,
        std::function<void(const std::exception &)> on_error = nullptr)
    {
        return std::make_shared<CallbackObserver<T>>(
            std::move(on_next),
            std::move(on_completed),
            std::move(on_error));
    }

    // Core Observable Interface - Source of values
    template <typename T>
    class IObservable
    {
    public:
        virtual std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) = 0;
        virtual void UnSubscribe(std::shared_ptr<IObserver<T>> observer) = 0;

    protected:
        virtual ~IObservable() = default;
    };

    // Subject Interface - Both Observer and Observable
    template <typename T>
    class ISubject : public IObservable<T>, public IObserver<T>
    {
    protected:
        virtual ~ISubject() = default;
    };

    // Base Operator Class - Provides common functionality for operators
    template <typename T>
    class Operator : public IObservable<T>, public std::enable_shared_from_this<Operator<T>>
    {
    protected:
        std::list<std::shared_ptr<IObserver<T>>> child_observers_;
        mutable std::mutex observers_mutex_;

    public:
        void NotifyOnNext(const T &value)
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

        void NotifyOnCompleted()
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

        void NotifyOnError(const std::exception &e)
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

        virtual std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            {
                std::lock_guard<std::mutex> lock(observers_mutex_);
                child_observers_.push_back(observer);
            }

            // Return subscription that removes observer when disposed
            auto weak_self = std::weak_ptr<Operator<T>>(std::static_pointer_cast<Operator<T>>(this->shared_from_this()));
            return std::make_shared<Subscription>([weak_self, observer]()
                                                  {
            if (auto self = weak_self.lock()) {
                self->UnSubscribe(observer);
            } });
        }

        virtual void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            std::lock_guard<std::mutex> lock(observers_mutex_);
            child_observers_.remove(observer);
        }
    };

} // namespace rx
