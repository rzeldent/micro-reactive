#ifndef MICRO_REACTIVE_ERROR_HANDLING_H
#define MICRO_REACTIVE_ERROR_HANDLING_H

#include "core.h"
#include <stdexcept>
#include <functional>

namespace rx {

// Custom exception types for reactive operations
class ReactiveException : public std::exception {
private:
    std::string message_;

public:
    ReactiveException(const std::string& message) : message_(message) {}
    const char* what() const noexcept override { return message_.c_str(); }
};

class SubscriptionException : public ReactiveException {
public:
    SubscriptionException(const std::string& message) 
        : ReactiveException("Subscription error: " + message) {}
};

class OperatorException : public ReactiveException {
public:
    OperatorException(const std::string& message) 
        : ReactiveException("Operator error: " + message) {}
};

class SchedulerException : public ReactiveException {
public:
    SchedulerException(const std::string& message) 
        : ReactiveException("Scheduler error: " + message) {}
};

// Error handling operators

// Catch operator - handles errors and provides fallback values
template<typename T>
class CatchOperator : public Operator<T> {
private:
    std::shared_ptr<IObservable<T>> source_;
    std::function<std::shared_ptr<IObservable<T>>(const std::exception&)> error_handler_;
    std::shared_ptr<Subscription> source_subscription_;
    std::shared_ptr<Subscription> fallback_subscription_;
    mutable std::mutex subscription_mutex_;

    class CatchObserver : public IObserver<T> {
    private:
        std::weak_ptr<CatchOperator<T>> parent_;

    public:
        CatchObserver(std::weak_ptr<CatchOperator<T>> parent) : parent_(parent) {}

        void OnNext(const T& value) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->NotifyOnCompleted();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->HandleError(e);
            }
        }
    };

    class FallbackObserver : public IObserver<T> {
    private:
        std::weak_ptr<CatchOperator<T>> parent_;

    public:
        FallbackObserver(std::weak_ptr<CatchOperator<T>> parent) : parent_(parent) {}

        void OnNext(const T& value) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->NotifyOnCompleted();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnError(e);
            }
        }
    };

public:
    CatchOperator(std::shared_ptr<IObservable<T>> source,
                 std::function<std::shared_ptr<IObservable<T>>(const std::exception&)> error_handler)
        : source_(source), error_handler_(error_handler) {}

    void HandleError(const std::exception& e) {
        try {
            auto fallback_source = error_handler_(e);
            if (fallback_source) {
                std::lock_guard<std::mutex> lock(subscription_mutex_);
                auto weak_self = std::weak_ptr<CatchOperator<T>>(
                    std::static_pointer_cast<CatchOperator<T>>(this->shared_from_this()));
                auto fallback_observer = std::make_shared<FallbackObserver>(weak_self);
                fallback_subscription_ = fallback_source->Subscribe(fallback_observer);
            } else {
                this->NotifyOnError(e);
            }
        } catch (const std::exception& handler_error) {
            this->NotifyOnError(handler_error);
        }
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto base_subscription = Operator<T>::Subscribe(observer);
        
        if (!source_subscription_) {
            auto weak_self = std::weak_ptr<CatchOperator<T>>(
                std::static_pointer_cast<CatchOperator<T>>(this->shared_from_this()));
            auto catch_observer = std::make_shared<CatchObserver>(weak_self);
            source_subscription_ = source_->Subscribe(catch_observer);
        }

        return base_subscription;
    }
};

// Retry operator - retries on error up to a specified number of times
template<typename T>
class RetryOperator : public Operator<T> {
private:
    std::shared_ptr<IObservable<T>> source_;
    int max_retries_;
    std::atomic<int> retry_count_;
    std::shared_ptr<Subscription> current_subscription_;
    mutable std::mutex subscription_mutex_;

    class RetryObserver : public IObserver<T> {
    private:
        std::weak_ptr<RetryOperator<T>> parent_;

    public:
        RetryObserver(std::weak_ptr<RetryOperator<T>> parent) : parent_(parent) {}

        void OnNext(const T& value) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->NotifyOnCompleted();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->HandleError(e);
            }
        }
    };

public:
    RetryOperator(std::shared_ptr<IObservable<T>> source, int max_retries)
        : source_(source), max_retries_(max_retries), retry_count_(0) {}

    void HandleError(const std::exception& e) {
        int current_retries = retry_count_.fetch_add(1);
        if (current_retries < max_retries_) {
            // Retry
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            auto weak_self = std::weak_ptr<RetryOperator<T>>(
                std::static_pointer_cast<RetryOperator<T>>(this->shared_from_this()));
            auto retry_observer = std::make_shared<RetryObserver>(weak_self);
            current_subscription_ = source_->Subscribe(retry_observer);
        } else {
            // Max retries exceeded
            this->NotifyOnError(e);
        }
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto base_subscription = Operator<T>::Subscribe(observer);
        
        if (!current_subscription_) {
            retry_count_.store(0);
            std::lock_guard<std::mutex> lock(subscription_mutex_);
            auto weak_self = std::weak_ptr<RetryOperator<T>>(
                std::static_pointer_cast<RetryOperator<T>>(this->shared_from_this()));
            auto retry_observer = std::make_shared<RetryObserver>(weak_self);
            current_subscription_ = source_->Subscribe(retry_observer);
        }

        return base_subscription;
    }
};

// Finally operator - executes an action when the observable terminates (either completes or errors)
template<typename T>
class FinallyOperator : public Operator<T> {
private:
    std::shared_ptr<IObservable<T>> source_;
    std::function<void()> finally_action_;
    std::shared_ptr<Subscription> source_subscription_;
    std::atomic<bool> finally_executed_;

    class FinallyObserver : public IObserver<T> {
    private:
        std::weak_ptr<FinallyOperator<T>> parent_;

    public:
        FinallyObserver(std::weak_ptr<FinallyOperator<T>> parent) : parent_(parent) {}

        void OnNext(const T& value) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->HandleTermination();
                p->NotifyOnCompleted();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->HandleTermination();
                p->NotifyOnError(e);
            }
        }
    };

public:
    FinallyOperator(std::shared_ptr<IObservable<T>> source, std::function<void()> finally_action)
        : source_(source), finally_action_(finally_action), finally_executed_(false) {}

    void HandleTermination() {
        if (!finally_executed_.exchange(true) && finally_action_) {
            try {
                finally_action_();
            } catch (const std::exception& e) {
                // Log error but don't propagate to avoid masking original error
                // In embedded systems, we might just ignore this
            }
        }
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto base_subscription = Operator<T>::Subscribe(observer);
        
        if (!source_subscription_) {
            finally_executed_.store(false);
            auto weak_self = std::weak_ptr<FinallyOperator<T>>(
                std::static_pointer_cast<FinallyOperator<T>>(this->shared_from_this()));
            auto finally_observer = std::make_shared<FinallyObserver>(weak_self);
            source_subscription_ = source_->Subscribe(finally_observer);
        }

        return base_subscription;
    }
};

// Factory functions for error handling operators
template<typename T>
std::shared_ptr<CatchOperator<T>> Catch(
    std::shared_ptr<IObservable<T>> source,
    std::function<std::shared_ptr<IObservable<T>>(const std::exception&)> error_handler) {
    return std::make_shared<CatchOperator<T>>(source, error_handler);
}

template<typename T>
std::shared_ptr<CatchOperator<T>> CatchAndReturn(
    std::shared_ptr<IObservable<T>> source,
    const T& fallback_value) {
    return std::make_shared<CatchOperator<T>>(source, 
        [fallback_value](const std::exception&) -> std::shared_ptr<IObservable<T>> {
            // Create a simple observable that emits the fallback value
            auto fallback_source = std::make_shared<Operator<T>>();
            // Schedule emission of fallback value
            std::thread([fallback_source, fallback_value]() {
                fallback_source->NotifyOnNext(fallback_value);
                fallback_source->NotifyOnCompleted();
            }).detach();
            return fallback_source;
        });
}

template<typename T>
std::shared_ptr<RetryOperator<T>> Retry(std::shared_ptr<IObservable<T>> source, int max_retries) {
    return std::make_shared<RetryOperator<T>>(source, max_retries);
}

template<typename T>
std::shared_ptr<FinallyOperator<T>> Finally(std::shared_ptr<IObservable<T>> source, 
                                           std::function<void()> finally_action) {
    return std::make_shared<FinallyOperator<T>>(source, finally_action);
}

// Safe observer wrapper that catches exceptions
template<typename T>
class SafeObserver : public IObserver<T> {
private:
    std::shared_ptr<IObserver<T>> inner_observer_;
    std::function<void(const std::exception&)> error_handler_;

public:
    SafeObserver(std::shared_ptr<IObserver<T>> inner_observer,
                std::function<void(const std::exception&)> error_handler = nullptr)
        : inner_observer_(inner_observer), error_handler_(error_handler) {}

    void OnNext(const T& value) override {
        try {
            if (inner_observer_) {
                inner_observer_->OnNext(value);
            }
        } catch (const std::exception& e) {
            HandleException(e);
        }
    }

    void OnCompleted() override {
        try {
            if (inner_observer_) {
                inner_observer_->OnCompleted();
            }
        } catch (const std::exception& e) {
            HandleException(e);
        }
    }

    void OnError(const std::exception& e) override {
        try {
            if (inner_observer_) {
                inner_observer_->OnError(e);
            }
        } catch (const std::exception& nested_e) {
            HandleException(nested_e);
        }
    }

private:
    void HandleException(const std::exception& e) {
        if (error_handler_) {
            try {
                error_handler_(e);
            } catch (...) {
                // Last resort - ignore exceptions in error handler
            }
        }
    }
};

// Factory function for safe observer
template<typename T>
std::shared_ptr<SafeObserver<T>> MakeSafeObserver(
    std::shared_ptr<IObserver<T>> observer,
    std::function<void(const std::exception&)> error_handler = nullptr) {
    return std::make_shared<SafeObserver<T>>(observer, error_handler);
}

} // namespace rx

#endif // MICRO_REACTIVE_ERROR_HANDLING_H
