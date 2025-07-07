#ifndef MICRO_REACTIVE_ADVANCED_OPERATORS_H
#define MICRO_REACTIVE_ADVANCED_OPERATORS_H

#include "core.h"
#include "scheduler.h"
#include <chrono>
#include <queue>
#include <set>

namespace rx {

// Debounce operator - emits only after a quiet period
template<typename T>
class DebounceOperator : public Operator<T> {
private:
    std::shared_ptr<IObservable<T>> source_;
    std::chrono::milliseconds delay_;
    std::shared_ptr<IScheduler> scheduler_;
    std::shared_ptr<IScheduledWork> pending_work_;
    std::shared_ptr<Subscription> source_subscription_;
    mutable std::mutex state_mutex_;
    T last_value_;
    bool has_pending_value_;

    class DebounceObserver : public IObserver<T> {
    private:
        std::weak_ptr<DebounceOperator<T>> parent_;

    public:
        DebounceObserver(std::weak_ptr<DebounceOperator<T>> parent) : parent_(parent) {}

        void OnNext(const T& value) override {
            if (auto p = parent_.lock()) {
                p->HandleValue(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->HandleCompleted();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnError(e);
            }
        }
    };

public:
    DebounceOperator(std::shared_ptr<IObservable<T>> source, 
                    std::chrono::milliseconds delay,
                    std::shared_ptr<IScheduler> scheduler = nullptr)
        : source_(source), delay_(delay), 
          scheduler_(scheduler ? scheduler : std::make_shared<ThreadPoolScheduler>()),
          has_pending_value_(false) {}

    void HandleValue(const T& value) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        
        // Cancel previous pending emission
        if (pending_work_ && !pending_work_->IsCancelled()) {
            pending_work_->Cancel();
        }

        last_value_ = value;
        has_pending_value_ = true;

        // Schedule new emission
        auto weak_self = std::weak_ptr<DebounceOperator<T>>(
            std::static_pointer_cast<DebounceOperator<T>>(this->shared_from_this()));
        
        pending_work_ = scheduler_->ScheduleDelayed([weak_self]() {
            if (auto self = weak_self.lock()) {
                self->EmitPendingValue();
            }
        }, delay_);
    }

    void HandleCompleted() {
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (pending_work_ && !pending_work_->IsCancelled()) {
                pending_work_->Cancel();
            }
            
            // Emit any pending value before completing
            if (has_pending_value_) {
                this->NotifyOnNext(last_value_);
            }
        }
        this->NotifyOnCompleted();
    }

    void EmitPendingValue() {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (has_pending_value_) {
            this->NotifyOnNext(last_value_);
            has_pending_value_ = false;
        }
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto base_subscription = Operator<T>::Subscribe(observer);
        
        if (!source_subscription_) {
            auto debounce_observer = std::make_shared<DebounceObserver>(
                std::static_pointer_cast<DebounceOperator<T>>(this->shared_from_this()));
            source_subscription_ = source_->Subscribe(debounce_observer);
        }

        return base_subscription;
    }
};

// CombineLatest operator - combines latest values from multiple sources
template<typename T1, typename T2, typename R>
class CombineLatestOperator : public Operator<R> {
private:
    std::shared_ptr<IObservable<T1>> source1_;
    std::shared_ptr<IObservable<T2>> source2_;
    std::function<R(const T1&, const T2&)> combiner_;
    
    mutable std::mutex state_mutex_;
    T1 value1_;
    T2 value2_;
    bool has_value1_;
    bool has_value2_;
    std::shared_ptr<Subscription> subscription1_;
    std::shared_ptr<Subscription> subscription2_;

    class Observer1 : public IObserver<T1> {
    private:
        std::weak_ptr<CombineLatestOperator<T1, T2, R>> parent_;

    public:
        Observer1(std::weak_ptr<CombineLatestOperator<T1, T2, R>> parent) : parent_(parent) {}

        void OnNext(const T1& value) override {
            if (auto p = parent_.lock()) {
                p->HandleValue1(value);
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

    class Observer2 : public IObserver<T2> {
    private:
        std::weak_ptr<CombineLatestOperator<T1, T2, R>> parent_;

    public:
        Observer2(std::weak_ptr<CombineLatestOperator<T1, T2, R>> parent) : parent_(parent) {}

        void OnNext(const T2& value) override {
            if (auto p = parent_.lock()) {
                p->HandleValue2(value);
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
    CombineLatestOperator(std::shared_ptr<IObservable<T1>> source1,
                         std::shared_ptr<IObservable<T2>> source2,
                         std::function<R(const T1&, const T2&)> combiner)
        : source1_(source1), source2_(source2), combiner_(combiner),
          has_value1_(false), has_value2_(false) {}

    void HandleValue1(const T1& value) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        value1_ = value;
        has_value1_ = true;
        
        if (has_value2_) {
            auto result = combiner_(value1_, value2_);
            this->NotifyOnNext(result);
        }
    }

    void HandleValue2(const T2& value) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        value2_ = value;
        has_value2_ = true;
        
        if (has_value1_) {
            auto result = combiner_(value1_, value2_);
            this->NotifyOnNext(result);
        }
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<R>> observer) override {
        auto base_subscription = Operator<R>::Subscribe(observer);
        
        if (!subscription1_ || !subscription2_) {
            auto weak_self = std::weak_ptr<CombineLatestOperator<T1, T2, R>>(
                std::static_pointer_cast<CombineLatestOperator<T1, T2, R>>(this->shared_from_this()));
            
            auto observer1 = std::make_shared<Observer1>(weak_self);
            auto observer2 = std::make_shared<Observer2>(weak_self);
            
            subscription1_ = source1_->Subscribe(observer1);
            subscription2_ = source2_->Subscribe(observer2);
        }

        return base_subscription;
    }
};

// Zip operator - combines values from multiple sources by pairing them
template<typename T1, typename T2, typename R>
class ZipOperator : public Operator<R> {
private:
    std::shared_ptr<IObservable<T1>> source1_;
    std::shared_ptr<IObservable<T2>> source2_;
    std::function<R(const T1&, const T2&)> combiner_;
    
    mutable std::mutex state_mutex_;
    std::queue<T1> queue1_;
    std::queue<T2> queue2_;
    std::shared_ptr<Subscription> subscription1_;
    std::shared_ptr<Subscription> subscription2_;
    bool source1_completed_;
    bool source2_completed_;

    class ZipObserver1 : public IObserver<T1> {
    private:
        std::weak_ptr<ZipOperator<T1, T2, R>> parent_;

    public:
        ZipObserver1(std::weak_ptr<ZipOperator<T1, T2, R>> parent) : parent_(parent) {}

        void OnNext(const T1& value) override {
            if (auto p = parent_.lock()) {
                p->HandleValue1(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->HandleCompleted1();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnError(e);
            }
        }
    };

    class ZipObserver2 : public IObserver<T2> {
    private:
        std::weak_ptr<ZipOperator<T1, T2, R>> parent_;

    public:
        ZipObserver2(std::weak_ptr<ZipOperator<T1, T2, R>> parent) : parent_(parent) {}

        void OnNext(const T2& value) override {
            if (auto p = parent_.lock()) {
                p->HandleValue2(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->HandleCompleted2();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnError(e);
            }
        }
    };

public:
    ZipOperator(std::shared_ptr<IObservable<T1>> source1,
               std::shared_ptr<IObservable<T2>> source2,
               std::function<R(const T1&, const T2&)> combiner)
        : source1_(source1), source2_(source2), combiner_(combiner),
          source1_completed_(false), source2_completed_(false) {}

    void HandleValue1(const T1& value) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        queue1_.push(value);
        TryEmitPair();
    }

    void HandleValue2(const T2& value) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        queue2_.push(value);
        TryEmitPair();
    }

    void HandleCompleted1() {
        std::lock_guard<std::mutex> lock(state_mutex_);
        source1_completed_ = true;
        CheckCompletion();
    }

    void HandleCompleted2() {
        std::lock_guard<std::mutex> lock(state_mutex_);
        source2_completed_ = true;
        CheckCompletion();
    }

    void TryEmitPair() {
        if (!queue1_.empty() && !queue2_.empty()) {
            auto value1 = queue1_.front();
            auto value2 = queue2_.front();
            queue1_.pop();
            queue2_.pop();
            
            auto result = combiner_(value1, value2);
            this->NotifyOnNext(result);
        }
    }

    void CheckCompletion() {
        if (source1_completed_ || source2_completed_ || queue1_.empty() || queue2_.empty()) {
            this->NotifyOnCompleted();
        }
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<R>> observer) override {
        auto base_subscription = Operator<R>::Subscribe(observer);
        
        if (!subscription1_ || !subscription2_) {
            auto weak_self = std::weak_ptr<ZipOperator<T1, T2, R>>(
                std::static_pointer_cast<ZipOperator<T1, T2, R>>(this->shared_from_this()));
            
            auto observer1 = std::make_shared<ZipObserver1>(weak_self);
            auto observer2 = std::make_shared<ZipObserver2>(weak_self);
            
            subscription1_ = source1_->Subscribe(observer1);
            subscription2_ = source2_->Subscribe(observer2);
        }

        return base_subscription;
    }
};

// Switch operator - switches to the latest inner observable
template<typename T, typename R>
class SwitchOperator : public Operator<R> {
private:
    std::shared_ptr<IObservable<T>> source_;
    std::function<std::shared_ptr<IObservable<R>>(const T&)> selector_;
    
    mutable std::mutex state_mutex_;
    std::shared_ptr<Subscription> source_subscription_;
    std::shared_ptr<Subscription> inner_subscription_;
    bool source_completed_;
    bool inner_completed_;

    class SwitchObserver : public IObserver<T> {
    private:
        std::weak_ptr<SwitchOperator<T, R>> parent_;

    public:
        SwitchObserver(std::weak_ptr<SwitchOperator<T, R>> parent) : parent_(parent) {}

        void OnNext(const T& value) override {
            if (auto p = parent_.lock()) {
                p->HandleOuterValue(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->HandleOuterCompleted();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnError(e);
            }
        }
    };

    class InnerObserver : public IObserver<R> {
    private:
        std::weak_ptr<SwitchOperator<T, R>> parent_;

    public:
        InnerObserver(std::weak_ptr<SwitchOperator<T, R>> parent) : parent_(parent) {}

        void OnNext(const R& value) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->HandleInnerCompleted();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnError(e);
            }
        }
    };

public:
    SwitchOperator(std::shared_ptr<IObservable<T>> source,
                  std::function<std::shared_ptr<IObservable<R>>(const T&)> selector)
        : source_(source), selector_(selector), source_completed_(false), inner_completed_(true) {}

    void HandleOuterValue(const T& value) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        
        // Unsubscribe from current inner observable
        if (inner_subscription_) {
            inner_subscription_->Dispose();
            inner_subscription_.reset();
        }

        // Subscribe to new inner observable
        auto inner_observable = selector_(value);
        if (inner_observable) {
            inner_completed_ = false;
            auto weak_self = std::weak_ptr<SwitchOperator<T, R>>(
                std::static_pointer_cast<SwitchOperator<T, R>>(this->shared_from_this()));
            auto inner_observer = std::make_shared<InnerObserver>(weak_self);
            inner_subscription_ = inner_observable->Subscribe(inner_observer);
        }
    }

    void HandleOuterCompleted() {
        std::lock_guard<std::mutex> lock(state_mutex_);
        source_completed_ = true;
        CheckCompletion();
    }

    void HandleInnerCompleted() {
        std::lock_guard<std::mutex> lock(state_mutex_);
        inner_completed_ = true;
        CheckCompletion();
    }

private:
    void CheckCompletion() {
        // Must be called with lock held
        if (source_completed_ && inner_completed_) {
            this->NotifyOnCompleted();
        }
    }

public:
    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<R>> observer) override {
        auto base_subscription = Operator<R>::Subscribe(observer);
        
        if (!source_subscription_) {
            auto weak_self = std::weak_ptr<SwitchOperator<T, R>>(
                std::static_pointer_cast<SwitchOperator<T, R>>(this->shared_from_this()));
            auto switch_observer = std::make_shared<SwitchObserver>(weak_self);
            source_subscription_ = source_->Subscribe(switch_observer);
        }

        return base_subscription;
    }
};

// FlatMap operator - flattens inner observables
template<typename T, typename R>
class FlatMapOperator : public Operator<R> {
private:
    std::shared_ptr<IObservable<T>> source_;
    std::function<std::shared_ptr<IObservable<R>>(const T&)> selector_;
    
    mutable std::mutex state_mutex_;
    std::shared_ptr<Subscription> source_subscription_;
    std::set<std::shared_ptr<Subscription>> inner_subscriptions_;
    std::atomic<int> active_inner_count_;
    bool source_completed_;

    class FlatMapObserver : public IObserver<T> {
    private:
        std::weak_ptr<FlatMapOperator<T, R>> parent_;

    public:
        FlatMapObserver(std::weak_ptr<FlatMapOperator<T, R>> parent) : parent_(parent) {}

        void OnNext(const T& value) override {
            if (auto p = parent_.lock()) {
                p->HandleOuterValue(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->HandleOuterCompleted();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnError(e);
            }
        }
    };

    class InnerObserver : public IObserver<R> {
    private:
        std::weak_ptr<FlatMapOperator<T, R>> parent_;
        std::shared_ptr<Subscription> subscription_;

    public:
        InnerObserver(std::weak_ptr<FlatMapOperator<T, R>> parent, std::shared_ptr<Subscription> sub) 
            : parent_(parent), subscription_(sub) {}

        void OnNext(const R& value) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->HandleInnerCompleted(subscription_);
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnError(e);
            }
        }
    };

public:
    FlatMapOperator(std::shared_ptr<IObservable<T>> source,
                   std::function<std::shared_ptr<IObservable<R>>(const T&)> selector)
        : source_(source), selector_(selector), active_inner_count_(0), source_completed_(false) {}

    void HandleOuterValue(const T& value) {
        auto inner_observable = selector_(value);
        if (inner_observable) {
            active_inner_count_.fetch_add(1);
            
            std::shared_ptr<Subscription> inner_subscription;
            auto weak_self = std::weak_ptr<FlatMapOperator<T, R>>(
                std::static_pointer_cast<FlatMapOperator<T, R>>(this->shared_from_this()));
            auto inner_observer = std::make_shared<InnerObserver>(weak_self, inner_subscription);
            inner_subscription = inner_observable->Subscribe(inner_observer);
            
            // Update the observer with the actual subscription
            inner_observer = std::make_shared<InnerObserver>(weak_self, inner_subscription);
            
            std::lock_guard<std::mutex> lock(state_mutex_);
            inner_subscriptions_.insert(inner_subscription);
        }
    }

    void HandleOuterCompleted() {
        source_completed_ = true;
        CheckCompletion();
    }

    void HandleInnerCompleted(std::shared_ptr<Subscription> subscription) {
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            inner_subscriptions_.erase(subscription);
        }
        
        active_inner_count_.fetch_sub(1);
        CheckCompletion();
    }

private:
    void CheckCompletion() {
        if (source_completed_ && active_inner_count_.load() == 0) {
            this->NotifyOnCompleted();
        }
    }

public:
    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<R>> observer) override {
        auto base_subscription = Operator<R>::Subscribe(observer);
        
        if (!source_subscription_) {
            auto weak_self = std::weak_ptr<FlatMapOperator<T, R>>(
                std::static_pointer_cast<FlatMapOperator<T, R>>(this->shared_from_this()));
            auto flatmap_observer = std::make_shared<FlatMapObserver>(weak_self);
            source_subscription_ = source_->Subscribe(flatmap_observer);
        }

        return base_subscription;
    }
};

// Concat operator - concatenates observables sequentially
template<typename T>
class ConcatOperator : public Operator<T> {
private:
    std::vector<std::shared_ptr<IObservable<T>>> sources_;
    mutable std::mutex state_mutex_;
    std::shared_ptr<Subscription> current_subscription_;
    size_t current_index_;

    class ConcatObserver : public IObserver<T> {
    private:
        std::weak_ptr<ConcatOperator<T>> parent_;

    public:
        ConcatObserver(std::weak_ptr<ConcatOperator<T>> parent) : parent_(parent) {}

        void OnNext(const T& value) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->HandleCurrentCompleted();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnError(e);
            }
        }
    };

public:
    ConcatOperator(std::vector<std::shared_ptr<IObservable<T>>> sources)
        : sources_(sources), current_index_(0) {}

    void HandleCurrentCompleted() {
        std::lock_guard<std::mutex> lock(state_mutex_);
        current_index_++;
        
        if (current_index_ >= sources_.size()) {
            this->NotifyOnCompleted();
        } else {
            SubscribeToNext();
        }
    }

private:
    void SubscribeToNext() {
        // Must be called with lock held
        if (current_index_ < sources_.size()) {
            auto weak_self = std::weak_ptr<ConcatOperator<T>>(
                std::static_pointer_cast<ConcatOperator<T>>(this->shared_from_this()));
            auto concat_observer = std::make_shared<ConcatObserver>(weak_self);
            current_subscription_ = sources_[current_index_]->Subscribe(concat_observer);
        }
    }

public:
    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto base_subscription = Operator<T>::Subscribe(observer);
        
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (!current_subscription_ && !sources_.empty()) {
                current_index_ = 0;
                SubscribeToNext();
            }
        }

        return base_subscription;
    }
};

// Sample operator - samples values at regular intervals
template<typename T>
class SampleOperator : public Operator<T> {
private:
    std::shared_ptr<IObservable<T>> source_;
    std::chrono::milliseconds interval_;
    std::shared_ptr<IScheduler> scheduler_;
    
    mutable std::mutex state_mutex_;
    T last_value_;
    bool has_value_;
    std::shared_ptr<IScheduledWork> timer_work_;
    std::shared_ptr<Subscription> source_subscription_;

    class SampleObserver : public IObserver<T> {
    private:
        std::weak_ptr<SampleOperator<T>> parent_;

    public:
        SampleObserver(std::weak_ptr<SampleOperator<T>> parent) : parent_(parent) {}

        void OnNext(const T& value) override {
            if (auto p = parent_.lock()) {
                p->HandleValue(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->HandleCompleted();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnError(e);
            }
        }
    };

public:
    SampleOperator(std::shared_ptr<IObservable<T>> source,
                  std::chrono::milliseconds interval,
                  std::shared_ptr<IScheduler> scheduler = nullptr)
        : source_(source), interval_(interval),
          scheduler_(scheduler ? scheduler : std::make_shared<ThreadPoolScheduler>()),
          has_value_(false) {}

    void HandleValue(const T& value) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        last_value_ = value;
        has_value_ = true;
    }

    void HandleCompleted() {
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (timer_work_) {
                timer_work_->Cancel();
            }
        }
        this->NotifyOnCompleted();
    }

    void EmitSample() {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (has_value_) {
            this->NotifyOnNext(last_value_);
        }
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto base_subscription = Operator<T>::Subscribe(observer);
        
        if (!source_subscription_) {
            auto weak_self = std::weak_ptr<SampleOperator<T>>(
                std::static_pointer_cast<SampleOperator<T>>(this->shared_from_this()));
            
            auto sample_observer = std::make_shared<SampleObserver>(weak_self);
            source_subscription_ = source_->Subscribe(sample_observer);
            
            // Start periodic sampling
            timer_work_ = scheduler_->SchedulePeriodic([weak_self]() {
                if (auto self = weak_self.lock()) {
                    self->EmitSample();
                }
            }, interval_);
        }

        return base_subscription;
    }
};

// Delay operator - delays emissions by a specified time
template<typename T>
class DelayOperator : public Operator<T> {
private:
    std::shared_ptr<IObservable<T>> source_;
    std::chrono::milliseconds delay_;
    std::shared_ptr<IScheduler> scheduler_;
    std::shared_ptr<Subscription> source_subscription_;

    class DelayObserver : public IObserver<T> {
    private:
        std::weak_ptr<DelayOperator<T>> parent_;

    public:
        DelayObserver(std::weak_ptr<DelayOperator<T>> parent) : parent_(parent) {}

        void OnNext(const T& value) override {
            if (auto p = parent_.lock()) {
                p->HandleValue(value);
            }
        }

        void OnCompleted() override {
            if (auto p = parent_.lock()) {
                p->HandleCompleted();
            }
        }

        void OnError(const std::exception& e) override {
            if (auto p = parent_.lock()) {
                p->NotifyOnError(e);
            }
        }
    };

public:
    DelayOperator(std::shared_ptr<IObservable<T>> source,
                 std::chrono::milliseconds delay,
                 std::shared_ptr<IScheduler> scheduler = nullptr)
        : source_(source), delay_(delay),
          scheduler_(scheduler ? scheduler : std::make_shared<ThreadPoolScheduler>()) {}

    void HandleValue(const T& value) {
        auto weak_self = std::weak_ptr<DelayOperator<T>>(
            std::static_pointer_cast<DelayOperator<T>>(this->shared_from_this()));
        
        scheduler_->ScheduleDelayed([weak_self, value]() {
            if (auto self = weak_self.lock()) {
                self->NotifyOnNext(value);
            }
        }, delay_);
    }

    void HandleCompleted() {
        auto weak_self = std::weak_ptr<DelayOperator<T>>(
            std::static_pointer_cast<DelayOperator<T>>(this->shared_from_this()));
        
        scheduler_->ScheduleDelayed([weak_self]() {
            if (auto self = weak_self.lock()) {
                self->NotifyOnCompleted();
            }
        }, delay_);
    }

    std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        auto base_subscription = Operator<T>::Subscribe(observer);
        
        if (!source_subscription_) {
            auto weak_self = std::weak_ptr<DelayOperator<T>>(
                std::static_pointer_cast<DelayOperator<T>>(this->shared_from_this()));
            auto delay_observer = std::make_shared<DelayObserver>(weak_self);
            source_subscription_ = source_->Subscribe(delay_observer);
        }

        return base_subscription;
    }
};

// Factory functions for advanced operators
template<typename T>
std::shared_ptr<DebounceOperator<T>> Debounce(
    std::shared_ptr<IObservable<T>> source, 
    std::chrono::milliseconds delay,
    std::shared_ptr<IScheduler> scheduler = nullptr) {
    return std::make_shared<DebounceOperator<T>>(source, delay, scheduler);
}

template<typename T1, typename T2, typename R>
std::shared_ptr<CombineLatestOperator<T1, T2, R>> CombineLatest(
    std::shared_ptr<IObservable<T1>> source1,
    std::shared_ptr<IObservable<T2>> source2,
    std::function<R(const T1&, const T2&)> combiner) {
    return std::make_shared<CombineLatestOperator<T1, T2, R>>(source1, source2, combiner);
}

// Zip operator factory
template<typename T1, typename T2, typename R>
std::shared_ptr<ZipOperator<T1, T2, R>> Zip(
    std::shared_ptr<IObservable<T1>> source1,
    std::shared_ptr<IObservable<T2>> source2,
    std::function<R(const T1&, const T2&)> zipper) {
    return std::make_shared<ZipOperator<T1, T2, R>>(source1, source2, zipper);
}

// Switch operator factory
template<typename T, typename R>
std::shared_ptr<SwitchOperator<T, R>> Switch(
    std::shared_ptr<IObservable<T>> source,
    std::function<std::shared_ptr<IObservable<R>>(const T&)> selector) {
    return std::make_shared<SwitchOperator<T, R>>(source, selector);
}

// FlatMap operator factory
template<typename T, typename R>
std::shared_ptr<FlatMapOperator<T, R>> FlatMap(
    std::shared_ptr<IObservable<T>> source,
    std::function<std::shared_ptr<IObservable<R>>(const T&)> selector) {
    return std::make_shared<FlatMapOperator<T, R>>(source, selector);
}

// SelectMany alias for FlatMap
template<typename T, typename R>
std::shared_ptr<FlatMapOperator<T, R>> SelectMany(
    std::shared_ptr<IObservable<T>> source,
    std::function<std::shared_ptr<IObservable<R>>(const T&)> selector) {
    return FlatMap<T, R>(source, selector);
}

// Concat operator factory
template<typename T>
std::shared_ptr<ConcatOperator<T>> Concat(
    std::vector<std::shared_ptr<IObservable<T>>> sources) {
    return std::make_shared<ConcatOperator<T>>(sources);
}

// Overload for two sources
template<typename T>
std::shared_ptr<ConcatOperator<T>> Concat(
    std::shared_ptr<IObservable<T>> source1,
    std::shared_ptr<IObservable<T>> source2) {
    std::vector<std::shared_ptr<IObservable<T>>> sources = {source1, source2};
    return std::make_shared<ConcatOperator<T>>(sources);
}

// Sample operator factory
template<typename T>
std::shared_ptr<SampleOperator<T>> Sample(
    std::shared_ptr<IObservable<T>> source,
    std::chrono::milliseconds interval,
    std::shared_ptr<IScheduler> scheduler = nullptr) {
    return std::make_shared<SampleOperator<T>>(source, interval, scheduler);
}

// Delay operator factory
template<typename T>
std::shared_ptr<DelayOperator<T>> Delay(
    std::shared_ptr<IObservable<T>> source,
    std::chrono::milliseconds delay,
    std::shared_ptr<IScheduler> scheduler = nullptr) {
    return std::make_shared<DelayOperator<T>>(source, delay, scheduler);
}

// WindowTime operator factory (alias for buffer with time)
template<typename T>
std::shared_ptr<SampleOperator<T>> WindowTime(
    std::shared_ptr<IObservable<T>> source,
    std::chrono::milliseconds time_span,
    std::shared_ptr<IScheduler> scheduler = nullptr) {
    return Sample<T>(source, time_span, scheduler);
}

} // namespace rx

#endif // MICRO_REACTIVE_ADVANCED_OPERATORS_H
