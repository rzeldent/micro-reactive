#pragma once

#include <core.h>
#include <scheduler.h>
#include <subjects.h>
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
#include <deque>
#include <stdexcept>

namespace rx
{
    namespace detail
    {
        class CompositeSubscriptions
        {
        private:
            std::mutex mutex_;
            std::vector<std::shared_ptr<Subscription>> subscriptions_;
            bool disposed_ = false;

        public:
            void Add(const std::shared_ptr<Subscription> &subscription)
            {
                if (!subscription)
                    return;
                bool dispose_now = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (disposed_)
                        dispose_now = true;
                    else
                        subscriptions_.push_back(subscription);
                }
                if (dispose_now)
                    subscription->Dispose();
            }

            void Dispose()
            {
                std::vector<std::shared_ptr<Subscription>> subscriptions;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (disposed_)
                        return;
                    disposed_ = true;
                    subscriptions.swap(subscriptions_);
                }
                for (const auto &subscription : subscriptions)
                    subscription->Dispose();
            }
        };
    }

    // Merge forwards values from all sources and completes after all complete.
    template <typename T>
    class MergeObservable : public IObservable<T>
    {
        class State : public std::enable_shared_from_this<State>
        {
            std::shared_ptr<IObserver<T>> downstream_;
            std::mutex mutex_;
            std::size_t remaining_;
            bool stopped_ = false;

        public:
            State(std::shared_ptr<IObserver<T>> downstream, std::size_t count)
                : downstream_(downstream), remaining_(count)
            {
            }

            std::shared_ptr<IObserver<T>> Observer()
            {
                std::weak_ptr<State> weak = this->shared_from_this();
                return CreateObserver<T>(
                    [weak](const T &value) {
                        if (auto self = weak.lock())
                            self->Next(value);
                    },
                    [weak]() {
                        if (auto self = weak.lock())
                            self->CompleteSource();
                    },
                    [weak](const std::exception &error) {
                        if (auto self = weak.lock())
                            self->Error(error);
                    });
            }

            void Next(const T &value)
            {
                std::shared_ptr<IObserver<T>> downstream;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    downstream = downstream_;
                }
                downstream->OnNext(value);
            }

            void CompleteSource()
            {
                std::shared_ptr<IObserver<T>> downstream;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_ || remaining_ == 0)
                        return;
                    if (--remaining_ == 0)
                    {
                        stopped_ = true;
                        downstream = downstream_;
                    }
                }
                if (downstream)
                    downstream->OnCompleted();
            }

            void Error(const std::exception &error)
            {
                std::shared_ptr<IObserver<T>> downstream;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                    downstream = downstream_;
                }
                downstream->OnError(error);
            }

            void CompleteEmpty()
            {
                std::shared_ptr<IObserver<T>> downstream;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                    downstream = downstream_;
                }
                downstream->OnCompleted();
            }

            void Dispose()
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stopped_ = true;
            }
        };

        std::vector<std::shared_ptr<IObservable<T>>> sources_;

    public:
        explicit MergeObservable(
            std::vector<std::shared_ptr<IObservable<T>>> sources)
            : sources_(std::move(sources))
        {
            for (const auto &source : sources_)
                if (!source)
                    throw std::invalid_argument(
                        "Merge source cannot be empty");
        }

        std::shared_ptr<Subscription> Subscribe(
            std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscriptions =
                std::make_shared<detail::CompositeSubscriptions>();
            auto state = std::make_shared<State>(observer, sources_.size());
            if (sources_.empty())
                state->CompleteEmpty();
            else
            {
                for (const auto &source : sources_)
                    subscriptions->Add(source->Subscribe(state->Observer()));
            }
            return std::make_shared<Subscription>(
                [subscriptions, state]() {
                    state->Dispose();
                    subscriptions->Dispose();
                });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>>) override {}
    };

    // Factory for merging a vector of same-typed sources.
    template <typename T>
    std::shared_ptr<IObservable<T>> Merge(
        std::vector<std::shared_ptr<IObservable<T>>> sources)
    {
        return std::make_shared<MergeObservable<T>>(std::move(sources));
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Merge(
        std::initializer_list<std::shared_ptr<IObservable<T>>> sources)
    {
        return Merge(std::vector<std::shared_ptr<IObservable<T>>>(sources));
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Merge(
        std::shared_ptr<IObservable<T>> first,
        std::shared_ptr<IObservable<T>> second)
    {
        return Merge<T>({first, second});
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Merge(
        std::shared_ptr<Subject<T>> first,
        std::shared_ptr<Subject<T>> second)
    {
        return Merge<T>(
            std::static_pointer_cast<IObservable<T>>(first),
            std::static_pointer_cast<IObservable<T>>(second));
    }

    // Zip pairs values by position and completes when no pair remains.
    template <typename TLeft, typename TRight>
    class ZipObservable : public IObservable<std::pair<TLeft, TRight>>
    {
        typedef std::pair<TLeft, TRight> Result;

        class State
        {
            std::shared_ptr<IObserver<Result>> downstream_;
            std::mutex mutex_;
            std::deque<TLeft> left_values_;
            std::deque<TRight> right_values_;
            bool left_completed_ = false;
            bool right_completed_ = false;
            bool stopped_ = false;

        public:
            explicit State(std::shared_ptr<IObserver<Result>> downstream)
                : downstream_(downstream)
            {
            }

            void Left(const TLeft &value)
            {
                std::shared_ptr<IObserver<Result>> downstream;
                std::shared_ptr<Result> result;
                bool emit = false;
                bool complete = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    left_values_.push_back(value);
                    if (!right_values_.empty())
                    {
                        result = std::make_shared<Result>(
                            left_values_.front(), right_values_.front());
                        left_values_.pop_front();
                        right_values_.pop_front();
                        downstream = downstream_;
                        emit = true;
                    }
                    if ((left_completed_ && left_values_.empty()) ||
                        (right_completed_ && right_values_.empty()))
                    {
                        stopped_ = true;
                        complete = true;
                    }
                }
                if (emit)
                    downstream->OnNext(*result);
                if (complete)
                    downstream_->OnCompleted();
            }

            void Right(const TRight &value)
            {
                std::shared_ptr<IObserver<Result>> downstream;
                std::shared_ptr<Result> result;
                bool emit = false;
                bool complete = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    right_values_.push_back(value);
                    if (!left_values_.empty())
                    {
                        result = std::make_shared<Result>(
                            left_values_.front(), right_values_.front());
                        left_values_.pop_front();
                        right_values_.pop_front();
                        downstream = downstream_;
                        emit = true;
                    }
                    if ((left_completed_ && left_values_.empty()) ||
                        (right_completed_ && right_values_.empty()))
                    {
                        stopped_ = true;
                        complete = true;
                    }
                }
                if (emit)
                    downstream->OnNext(*result);
                if (complete)
                    downstream_->OnCompleted();
            }

            void Complete(bool left)
            {
                bool notify = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    (left ? left_completed_ : right_completed_) = true;
                    if ((left_completed_ && left_values_.empty()) ||
                        (right_completed_ && right_values_.empty()))
                    {
                        stopped_ = true;
                        notify = true;
                    }
                }
                if (notify)
                    downstream_->OnCompleted();
            }

            void Error(const std::exception &error)
            {
                bool notify = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                    notify = true;
                }
                if (notify)
                    downstream_->OnError(error);
            }

            void Dispose()
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stopped_ = true;
            }
        };

        std::shared_ptr<IObservable<TLeft>> left_;
        std::shared_ptr<IObservable<TRight>> right_;

    public:
        ZipObservable(std::shared_ptr<IObservable<TLeft>> left,
                      std::shared_ptr<IObservable<TRight>> right)
            : left_(left), right_(right)
        {
            if (!left_ || !right_)
                throw std::invalid_argument(
                    "Zip requires two valid sources");
        }

        std::shared_ptr<Subscription> Subscribe(
            std::shared_ptr<IObserver<Result>> observer) override
        {
            auto subscriptions =
                std::make_shared<detail::CompositeSubscriptions>();
            auto state = std::make_shared<State>(observer);
            subscriptions->Add(left_->Subscribe(CreateObserver<TLeft>(
                [state](const TLeft &value) { state->Left(value); },
                [state]() { state->Complete(true); },
                [state](const std::exception &error) {
                    state->Error(error);
                })));
            subscriptions->Add(right_->Subscribe(CreateObserver<TRight>(
                [state](const TRight &value) { state->Right(value); },
                [state]() { state->Complete(false); },
                [state](const std::exception &error) {
                    state->Error(error);
                })));
            return std::make_shared<Subscription>(
                [subscriptions, state]() {
                    state->Dispose();
                    subscriptions->Dispose();
                });
        }

        void UnSubscribe(std::shared_ptr<IObserver<Result>>) override {}
    };

    // Factory for pairing items at matching positions from two sources.
    template <typename TLeft, typename TRight>
    std::shared_ptr<IObservable<std::pair<TLeft, TRight>>> Zip(
        std::shared_ptr<IObservable<TLeft>> left,
        std::shared_ptr<IObservable<TRight>> right)
    {
        return std::make_shared<ZipObservable<TLeft, TRight>>(left, right);
    }

    template <typename TLeft, typename TRight>
    std::shared_ptr<IObservable<std::pair<TLeft, TRight>>> Zip(
        std::shared_ptr<Subject<TLeft>> left,
        std::shared_ptr<Subject<TRight>> right)
    {
        return Zip<TLeft, TRight>(
            std::static_pointer_cast<IObservable<TLeft>>(left),
            std::static_pointer_cast<IObservable<TRight>>(right));
    }

    // FlatMap maps each source item to an observable and merges inner streams.
    template <typename TSource, typename TResult>
    class FlatMapObservable : public IObservable<TResult>
    {
        class State : public std::enable_shared_from_this<State>
        {
            std::shared_ptr<IObserver<TResult>> downstream_;
            std::shared_ptr<detail::CompositeSubscriptions> subscriptions_;
            std::mutex mutex_;
            std::size_t active_ = 1;
            bool stopped_ = false;

        public:
            State(std::shared_ptr<IObserver<TResult>> downstream,
                  std::shared_ptr<detail::CompositeSubscriptions> subscriptions)
                : downstream_(downstream), subscriptions_(subscriptions)
            {
            }

            void SubscribeInner(std::shared_ptr<IObservable<TResult>> inner)
            {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    ++active_;
                }
                std::weak_ptr<State> weak = this->shared_from_this();
                subscriptions_->Add(inner->Subscribe(CreateObserver<TResult>(
                    [weak](const TResult &value) {
                        if (auto self = weak.lock())
                            self->Next(value);
                    },
                    [weak]() {
                        if (auto self = weak.lock())
                            self->CompleteInner();
                    },
                    [weak](const std::exception &error) {
                        if (auto self = weak.lock())
                            self->Error(error);
                    })));
            }

            void Next(const TResult &value)
            {
                std::shared_ptr<IObserver<TResult>> downstream;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    downstream = downstream_;
                }
                downstream->OnNext(value);
            }

            void CompleteInner()
            {
                bool complete = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_ || active_ == 0)
                        return;
                    complete = --active_ == 0;
                    if (complete)
                        stopped_ = true;
                }
                if (complete)
                    downstream_->OnCompleted();
            }

            void Error(const std::exception &error)
            {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                }
                downstream_->OnError(error);
            }

            void Dispose()
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stopped_ = true;
            }
        };

        std::shared_ptr<IObservable<TSource>> source_;
        std::function<std::shared_ptr<IObservable<TResult>>(
            const TSource &)> mapper_;

    public:
        FlatMapObservable(
            std::shared_ptr<IObservable<TSource>> source,
            std::function<std::shared_ptr<IObservable<TResult>>(
                const TSource &)> mapper)
            : source_(source), mapper_(mapper)
        {
            if (!source_ || !mapper_)
                throw std::invalid_argument(
                    "FlatMap requires a source and mapper");
        }

        std::shared_ptr<Subscription> Subscribe(
            std::shared_ptr<IObserver<TResult>> observer) override
        {
            auto subscriptions =
                std::make_shared<detail::CompositeSubscriptions>();
            auto state = std::make_shared<State>(observer, subscriptions);
            auto mapper = mapper_;
            subscriptions->Add(source_->Subscribe(CreateObserver<TSource>(
                [state, mapper](const TSource &value) {
                    try
                    {
                        auto inner = mapper(value);
                        if (!inner)
                            throw std::invalid_argument(
                                "FlatMap mapper returned an empty observable");
                        state->SubscribeInner(inner);
                    }
                    catch (const std::exception &error)
                    {
                        state->Error(error);
                    }
                },
                [state]() { state->CompleteInner(); },
                [state](const std::exception &error) {
                    state->Error(error);
                })));
            return std::make_shared<Subscription>(
                [subscriptions, state]() {
                    state->Dispose();
                    subscriptions->Dispose();
                });
        }

        void UnSubscribe(std::shared_ptr<IObserver<TResult>>) override {}
    };

    template <typename TSource, typename TResult>
    std::shared_ptr<IObservable<TResult>> FlatMap(
        std::shared_ptr<IObservable<TSource>> source,
        std::function<std::shared_ptr<IObservable<TResult>>(
            const TSource &)> mapper)
    {
        return std::make_shared<FlatMapObservable<TSource, TResult>>(
            source, mapper);
    }

    template <typename TSource, typename TResult>
    std::shared_ptr<IObservable<TResult>> FlatMap(
        std::shared_ptr<Subject<TSource>> source,
        std::function<std::shared_ptr<IObservable<TResult>>(
            const TSource &)> mapper)
    {
        return FlatMap<TSource, TResult>(
            std::static_pointer_cast<IObservable<TSource>>(source), mapper);
    }

    // Concat subscribes to each source in order after the prior completes.
    template <typename T>
    class ConcatObservable : public IObservable<T>
    {
        class State : public std::enable_shared_from_this<State>
        {
            std::shared_ptr<IObserver<T>> downstream_;
            std::vector<std::shared_ptr<IObservable<T>>> sources_;
            std::shared_ptr<detail::CompositeSubscriptions> subscriptions_;
            std::mutex mutex_;
            std::size_t index_ = 0;
            bool stopped_ = false;
            bool subscribing_ = false;
            bool advance_requested_ = false;

            void SubscribeNext()
            {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    if (subscribing_)
                    {
                        advance_requested_ = true;
                        return;
                    }
                    subscribing_ = true;
                }

                while (true)
                {
                    std::shared_ptr<IObservable<T>> source;
                    bool complete = false;
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        if (stopped_)
                        {
                            subscribing_ = false;
                            return;
                        }
                        if (index_ == sources_.size())
                        {
                            stopped_ = true;
                            subscribing_ = false;
                            complete = true;
                        }
                        else
                            source = sources_[index_++];
                    }
                    if (complete)
                    {
                        downstream_->OnCompleted();
                        return;
                    }

                    std::weak_ptr<State> weak = this->shared_from_this();
                    subscriptions_->Add(source->Subscribe(CreateObserver<T>(
                        [weak](const T &value) {
                            if (auto self = weak.lock())
                                self->Next(value);
                        },
                        [weak]() {
                            if (auto self = weak.lock())
                                self->SubscribeNext();
                        },
                        [weak](const std::exception &error) {
                            if (auto self = weak.lock())
                                self->Error(error);
                        })));

                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        if (!advance_requested_)
                        {
                            subscribing_ = false;
                            return;
                        }
                        advance_requested_ = false;
                    }
                }
            }

        public:
            State(std::shared_ptr<IObserver<T>> downstream,
                  std::vector<std::shared_ptr<IObservable<T>>> sources,
                  std::shared_ptr<detail::CompositeSubscriptions> subscriptions)
                : downstream_(downstream), sources_(std::move(sources)),
                  subscriptions_(subscriptions)
            {
            }

            void Start()
            {
                SubscribeNext();
            }

            void Next(const T &value)
            {
                std::shared_ptr<IObserver<T>> downstream;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    downstream = downstream_;
                }
                downstream->OnNext(value);
            }

            void Error(const std::exception &error)
            {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                }
                downstream_->OnError(error);
            }

            void Dispose()
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stopped_ = true;
            }
        };

        std::vector<std::shared_ptr<IObservable<T>>> sources_;

    public:
        explicit ConcatObservable(
            std::vector<std::shared_ptr<IObservable<T>>> sources)
            : sources_(std::move(sources))
        {
            for (const auto &source : sources_)
                if (!source)
                    throw std::invalid_argument(
                        "Concat source cannot be empty");
        }

        std::shared_ptr<Subscription> Subscribe(
            std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscriptions =
                std::make_shared<detail::CompositeSubscriptions>();
            auto state = std::make_shared<State>(
                observer, sources_, subscriptions);
            state->Start();
            return std::make_shared<Subscription>(
                [subscriptions, state]() {
                    state->Dispose();
                    subscriptions->Dispose();
                });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>>) override {}
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Concat(
        std::vector<std::shared_ptr<IObservable<T>>> sources)
    {
        return std::make_shared<ConcatObservable<T>>(std::move(sources));
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Concat(
        std::initializer_list<std::shared_ptr<IObservable<T>>> sources)
    {
        return Concat(std::vector<std::shared_ptr<IObservable<T>>>(sources));
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Concat(
        std::shared_ptr<IObservable<T>> first,
        std::shared_ptr<IObservable<T>> second)
    {
        return Concat<T>({first, second});
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Concat(
        std::shared_ptr<Subject<T>> first,
        std::shared_ptr<Subject<T>> second)
    {
        return Concat<T>(
            std::static_pointer_cast<IObservable<T>>(first),
            std::static_pointer_cast<IObservable<T>>(second));
    }

    // Switch forwards only values from the most recently selected inner source.
    template <typename T>
    class SwitchObservable : public IObservable<T>
    {
        typedef std::shared_ptr<IObservable<T>> Inner;

        class State : public std::enable_shared_from_this<State>
        {
            std::shared_ptr<IObserver<T>> downstream_;
            std::shared_ptr<detail::CompositeSubscriptions> subscriptions_;
            std::mutex mutex_;
            std::shared_ptr<Subscription> inner_subscription_;
            std::size_t generation_ = 0;
            bool outer_completed_ = false;
            bool inner_active_ = false;
            bool stopped_ = false;

        public:
            State(std::shared_ptr<IObserver<T>> downstream,
                  std::shared_ptr<detail::CompositeSubscriptions> subscriptions)
                : downstream_(downstream), subscriptions_(subscriptions)
            {
            }

            void Select(const Inner &inner)
            {
                if (!inner)
                {
                    std::invalid_argument error(
                        "Switch received an empty inner source");
                    Error(error);
                    return;
                }
                std::shared_ptr<Subscription> previous;
                std::size_t generation;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    generation = ++generation_;
                    inner_active_ = true;
                    previous = inner_subscription_;
                    inner_subscription_.reset();
                }
                if (previous)
                    previous->Dispose();
                std::weak_ptr<State> weak = this->shared_from_this();
                auto subscription = inner->Subscribe(CreateObserver<T>(
                    [weak, generation](const T &value) {
                        if (auto self = weak.lock())
                            self->Next(generation, value);
                    },
                    [weak, generation]() {
                        if (auto self = weak.lock())
                            self->InnerCompleted(generation);
                    },
                    [weak](const std::exception &error) {
                        if (auto self = weak.lock())
                            self->Error(error);
                    }));
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (!stopped_ && generation == generation_)
                        inner_subscription_ = subscription;
                    else
                        subscription->Dispose();
                }
                subscriptions_->Add(subscription);
            }

            void Next(std::size_t generation, const T &value)
            {
                std::shared_ptr<IObserver<T>> downstream;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_ || generation != generation_)
                        return;
                    downstream = downstream_;
                }
                downstream->OnNext(value);
            }

            void InnerCompleted(std::size_t generation)
            {
                bool complete = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_ || generation != generation_)
                        return;
                    inner_active_ = false;
                    inner_subscription_.reset();
                    if (outer_completed_)
                    {
                        stopped_ = true;
                        complete = true;
                    }
                }
                if (complete)
                    downstream_->OnCompleted();
            }

            void OuterCompleted()
            {
                bool complete = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    outer_completed_ = true;
                    if (!inner_active_)
                    {
                        stopped_ = true;
                        complete = true;
                    }
                }
                if (complete)
                    downstream_->OnCompleted();
            }

            void Error(const std::exception &error)
            {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                }
                downstream_->OnError(error);
            }

            void Dispose()
            {
                std::shared_ptr<Subscription> inner;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    stopped_ = true;
                    inner = inner_subscription_;
                    inner_subscription_.reset();
                }
                if (inner)
                    inner->Dispose();
            }
        };

        std::shared_ptr<IObservable<Inner>> source_;

    public:
        explicit SwitchObservable(std::shared_ptr<IObservable<Inner>> source)
            : source_(source)
        {
            if (!source_)
                throw std::invalid_argument(
                    "Switch requires a valid outer source");
        }

        std::shared_ptr<Subscription> Subscribe(
            std::shared_ptr<IObserver<T>> observer) override
        {
            auto subscriptions =
                std::make_shared<detail::CompositeSubscriptions>();
            auto state = std::make_shared<State>(observer, subscriptions);
            std::weak_ptr<State> weak = state;
            subscriptions->Add(source_->Subscribe(CreateObserver<Inner>(
                [weak](const Inner &inner) {
                    if (auto self = weak.lock())
                        self->Select(inner);
                },
                [weak]() {
                    if (auto self = weak.lock())
                        self->OuterCompleted();
                },
                [weak](const std::exception &error) {
                    if (auto self = weak.lock())
                        self->Error(error);
                })));
            return std::make_shared<Subscription>(
                [subscriptions, state]() {
                    state->Dispose();
                    subscriptions->Dispose();
                });
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>>) override {}
    };

    template <typename T>
    std::shared_ptr<IObservable<T>> Switch(
        std::shared_ptr<IObservable<std::shared_ptr<IObservable<T>>>> source)
    {
        return std::make_shared<SwitchObservable<T>>(source);
    }

    template <typename T>
    std::shared_ptr<IObservable<T>> Switch(
        std::shared_ptr<Subject<std::shared_ptr<IObservable<T>>>> source)
    {
        return Switch<T>(
            std::static_pointer_cast<
                IObservable<std::shared_ptr<IObservable<T>>>>(source));
    }

    // WithLatestFrom emits a pair whenever the primary source emits after the
    // secondary source has produced its first value.
    template <typename TSource, typename TOther>
    class WithLatestFromObservable
        : public IObservable<std::pair<TSource, TOther>>
    {
        typedef std::pair<TSource, TOther> Result;

        class State
        {
            std::shared_ptr<IObserver<Result>> downstream_;
            std::mutex mutex_;
            std::shared_ptr<TOther> latest_;
            bool has_latest_ = false;
            bool stopped_ = false;

        public:
            explicit State(std::shared_ptr<IObserver<Result>> downstream)
                : downstream_(downstream)
            {
            }

            void Other(const TOther &value)
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (!stopped_)
                {
                    latest_ = std::make_shared<TOther>(value);
                    has_latest_ = true;
                }
            }

            void Source(const TSource &value)
            {
                std::shared_ptr<IObserver<Result>> downstream;
                std::shared_ptr<Result> result;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_ || !has_latest_)
                        return;
                    result = std::make_shared<Result>(value, *latest_);
                    downstream = downstream_;
                }
                downstream->OnNext(*result);
            }

            void Complete()
            {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                }
                downstream_->OnCompleted();
            }

            void Error(const std::exception &error)
            {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (stopped_)
                        return;
                    stopped_ = true;
                }
                downstream_->OnError(error);
            }

            void Dispose()
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stopped_ = true;
            }
        };

        std::shared_ptr<IObservable<TSource>> source_;
        std::shared_ptr<IObservable<TOther>> other_;

    public:
        WithLatestFromObservable(std::shared_ptr<IObservable<TSource>> source,
                                 std::shared_ptr<IObservable<TOther>> other)
            : source_(source), other_(other)
        {
            if (!source_ || !other_)
                throw std::invalid_argument(
                    "WithLatestFrom requires two valid sources");
        }

        std::shared_ptr<Subscription> Subscribe(
            std::shared_ptr<IObserver<Result>> observer) override
        {
            auto subscriptions =
                std::make_shared<detail::CompositeSubscriptions>();
            auto state = std::make_shared<State>(observer);
            subscriptions->Add(other_->Subscribe(CreateObserver<TOther>(
                [state](const TOther &value) { state->Other(value); },
                []() {},
                [state](const std::exception &error) {
                    state->Error(error);
                })));
            subscriptions->Add(source_->Subscribe(CreateObserver<TSource>(
                [state](const TSource &value) { state->Source(value); },
                [state]() { state->Complete(); },
                [state](const std::exception &error) {
                    state->Error(error);
                })));
            return std::make_shared<Subscription>(
                [subscriptions, state]() {
                    state->Dispose();
                    subscriptions->Dispose();
                });
        }

        void UnSubscribe(std::shared_ptr<IObserver<Result>>) override {}
    };

    template <typename TSource, typename TOther>
    std::shared_ptr<IObservable<std::pair<TSource, TOther>>> WithLatestFrom(
        std::shared_ptr<IObservable<TSource>> source,
        std::shared_ptr<IObservable<TOther>> other)
    {
        return std::make_shared<WithLatestFromObservable<TSource, TOther>>(
            source, other);
    }

    template <typename TSource, typename TOther>
    std::shared_ptr<IObservable<std::pair<TSource, TOther>>> WithLatestFrom(
        std::shared_ptr<Subject<TSource>> source,
        std::shared_ptr<Subject<TOther>> other)
    {
        return WithLatestFrom<TSource, TOther>(
            std::static_pointer_cast<IObservable<TSource>>(source),
            std::static_pointer_cast<IObservable<TOther>>(other));
    }

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
