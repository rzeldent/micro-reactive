#ifndef MICRO_REACTIVE_SCHEDULER_H
#define MICRO_REACTIVE_SCHEDULER_H

#include <functional>
#include <memory>
#include <chrono>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace rx
{

    // Forward declarations
    class IScheduler;
    class IScheduledWork;

    // Interface for scheduled work items
    class IScheduledWork
    {
    public:
        virtual void Execute() = 0;
        virtual bool IsCancelled() const = 0;
        virtual void Cancel() = 0;
        virtual ~IScheduledWork() = default;
    };

    // Basic scheduled work implementation
    class ScheduledWork : public IScheduledWork
    {
    private:
        std::function<void()> action_;
        std::atomic<bool> cancelled_;

    public:
        ScheduledWork(std::function<void()> action)
            : action_(std::move(action)), cancelled_(false) {}

        void Execute() override
        {
            if (!cancelled_.load() && action_)
            {
                try
                {
                    action_();
                }
                catch (const std::exception &e)
                {
                    // Log error or handle as needed
                    // For embedded systems, we might just ignore or set a flag
                }
            }
        }

        bool IsCancelled() const override
        {
            return cancelled_.load();
        }

        void Cancel() override
        {
            cancelled_.store(true);
        }
    };

    // Scheduler interface for controlling execution context
    class IScheduler
    {
    public:
        virtual std::shared_ptr<IScheduledWork> Schedule(std::function<void()> action) = 0;
        virtual std::shared_ptr<IScheduledWork> ScheduleDelayed(
            std::function<void()> action,
            std::chrono::milliseconds delay) = 0;
        virtual std::shared_ptr<IScheduledWork> SchedulePeriodic(
            std::function<void()> action,
            std::chrono::milliseconds period) = 0;
        virtual ~IScheduler() = default;
    };

    // Immediate scheduler - executes work immediately on current thread
    class ImmediateScheduler : public IScheduler
    {
    public:
        std::shared_ptr<IScheduledWork> Schedule(std::function<void()> action) override
        {
            auto work = std::make_shared<ScheduledWork>(action);
            work->Execute();
            return work;
        }

        std::shared_ptr<IScheduledWork> ScheduleDelayed(
            std::function<void()> action,
            std::chrono::milliseconds delay) override
        {
            // For immediate scheduler, we ignore delay and execute immediately
            return Schedule(action);
        }

        std::shared_ptr<IScheduledWork> SchedulePeriodic(
            std::function<void()> action,
            std::chrono::milliseconds period) override
        {
            // For immediate scheduler, we only execute once
            return Schedule(action);
        }
    };

    // Thread pool scheduler for background execution
    class ThreadPoolScheduler : public IScheduler
    {
    private:
        struct WorkItem
        {
            std::shared_ptr<IScheduledWork> work;
            std::chrono::steady_clock::time_point when;
            std::chrono::milliseconds period;
            bool is_periodic;

            WorkItem(std::shared_ptr<IScheduledWork> w,
                     std::chrono::steady_clock::time_point t,
                     std::chrono::milliseconds p = std::chrono::milliseconds::zero(),
                     bool periodic = false)
                : work(w), when(t), period(p), is_periodic(periodic) {}
        };

        std::queue<WorkItem> work_queue_;
        std::mutex queue_mutex_;
        std::condition_variable condition_;
        std::atomic<bool> shutdown_;
        std::thread worker_thread_;

        void WorkerLoop()
        {
            while (!shutdown_.load())
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);

                if (work_queue_.empty())
                {
                    condition_.wait_for(lock, std::chrono::milliseconds(100));
                    continue;
                }

                auto now = std::chrono::steady_clock::now();
                auto &item = work_queue_.front();

                if (item.when <= now)
                {
                    auto work_item = item;
                    work_queue_.pop();
                    lock.unlock();

                    // Execute the work
                    work_item.work->Execute();

                    // Re-schedule if periodic
                    if (work_item.is_periodic && !work_item.work->IsCancelled())
                    {
                        lock.lock();
                        work_item.when = now + work_item.period;
                        work_queue_.push(work_item);
                        lock.unlock();
                    }
                }
                else
                {
                    // Wait until next work item is ready
                    condition_.wait_until(lock, item.when);
                }
            }
        }

    public:
        ThreadPoolScheduler() : shutdown_(false)
        {
            worker_thread_ = std::thread(&ThreadPoolScheduler::WorkerLoop, this);
        }

        ~ThreadPoolScheduler()
        {
            shutdown_.store(true);
            condition_.notify_all();
            if (worker_thread_.joinable())
            {
                worker_thread_.join();
            }
        }

        std::shared_ptr<IScheduledWork> Schedule(std::function<void()> action) override
        {
            auto work = std::make_shared<ScheduledWork>(action);
            auto now = std::chrono::steady_clock::now();

            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                work_queue_.emplace(work, now);
            }
            condition_.notify_one();

            return work;
        }

        std::shared_ptr<IScheduledWork> ScheduleDelayed(
            std::function<void()> action,
            std::chrono::milliseconds delay) override
        {
            auto work = std::make_shared<ScheduledWork>(action);
            auto when = std::chrono::steady_clock::now() + delay;

            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                work_queue_.emplace(work, when);
            }
            condition_.notify_one();

            return work;
        }

        std::shared_ptr<IScheduledWork> SchedulePeriodic(
            std::function<void()> action,
            std::chrono::milliseconds period) override
        {
            auto work = std::make_shared<ScheduledWork>(action);
            auto when = std::chrono::steady_clock::now() + period;

            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                work_queue_.emplace(work, when, period, true);
            }
            condition_.notify_one();

            return work;
        }
    };

    // Test scheduler for deterministic testing
    class TestScheduler : public IScheduler
    {
    private:
        struct ScheduledAction
        {
            std::chrono::milliseconds when;
            std::shared_ptr<IScheduledWork> work;
            std::chrono::milliseconds period;
            bool is_periodic;

            ScheduledAction(std::chrono::milliseconds w, std::shared_ptr<IScheduledWork> work_item,
                           std::chrono::milliseconds p = std::chrono::milliseconds::zero(),
                           bool periodic = false)
                : when(w), work(work_item), period(p), is_periodic(periodic) {}

            bool operator<(const ScheduledAction& other) const
            {
                return when > other.when; // Reverse order for priority queue (min-heap)
            }
        };

        std::chrono::milliseconds virtual_time_;
        std::priority_queue<ScheduledAction> actions_;
        mutable std::mutex scheduler_mutex_;

    public:
        TestScheduler() : virtual_time_(0) {}

        // Get current virtual time
        std::chrono::milliseconds GetVirtualTime() const
        {
            std::lock_guard<std::mutex> lock(scheduler_mutex_);
            return virtual_time_;
        }

        // Advance virtual time by duration and execute due actions
        void AdvanceBy(std::chrono::milliseconds duration)
        {
            std::lock_guard<std::mutex> lock(scheduler_mutex_);
            auto target_time = virtual_time_ + duration;
            AdvanceToInternal(target_time);
        }

        // Advance virtual time to specific time and execute due actions
        void AdvanceTo(std::chrono::milliseconds time)
        {
            std::lock_guard<std::mutex> lock(scheduler_mutex_);
            if (time >= virtual_time_)
            {
                AdvanceToInternal(time);
            }
        }

        // Execute all scheduled actions
        void Start()
        {
            std::lock_guard<std::mutex> lock(scheduler_mutex_);
            while (!actions_.empty())
            {
                auto action = actions_.top();
                actions_.pop();
                virtual_time_ = action.when;
                
                if (!action.work->IsCancelled())
                {
                    action.work->Execute();
                    
                    if (action.is_periodic && !action.work->IsCancelled())
                    {
                        // Re-schedule periodic action
                        auto next_time = action.when + action.period;
                        actions_.emplace(next_time, action.work, action.period, true);
                    }
                }
            }
        }

        // Stop all scheduled actions
        void Stop()
        {
            std::lock_guard<std::mutex> lock(scheduler_mutex_);
            while (!actions_.empty())
            {
                actions_.top().work->Cancel();
                actions_.pop();
            }
        }

        // IScheduler implementation
        std::shared_ptr<IScheduledWork> Schedule(std::function<void()> action) override
        {
            return ScheduleDelayed(action, std::chrono::milliseconds::zero());
        }

        std::shared_ptr<IScheduledWork> ScheduleDelayed(
            std::function<void()> action,
            std::chrono::milliseconds delay) override
        {
            std::lock_guard<std::mutex> lock(scheduler_mutex_);
            auto work = std::make_shared<ScheduledWork>(action);
            auto when = virtual_time_ + delay;
            actions_.emplace(when, work);
            return work;
        }

        std::shared_ptr<IScheduledWork> SchedulePeriodic(
            std::function<void()> action,
            std::chrono::milliseconds period) override
        {
            std::lock_guard<std::mutex> lock(scheduler_mutex_);
            auto work = std::make_shared<ScheduledWork>(action);
            auto when = virtual_time_ + period;
            actions_.emplace(when, work, period, true);
            return work;
        }

    private:
        void AdvanceToInternal(std::chrono::milliseconds target_time)
        {
            while (!actions_.empty() && actions_.top().when <= target_time)
            {
                auto action = actions_.top();
                actions_.pop();
                virtual_time_ = action.when;
                
                if (!action.work->IsCancelled())
                {
                    action.work->Execute();
                    
                    if (action.is_periodic && !action.work->IsCancelled())
                    {
                        // Re-schedule periodic action
                        auto next_time = action.when + action.period;
                        if (next_time <= target_time)
                        {
                            actions_.emplace(next_time, action.work, action.period, true);
                        }
                        else
                        {
                            actions_.emplace(next_time, action.work, action.period, true);
                        }
                    }
                }
            }
            virtual_time_ = target_time;
        }
    };

    // Default schedulers
    namespace Schedulers
    {
        inline ImmediateScheduler &Immediate()
        {
            static ImmediateScheduler instance;
            return instance;
        }

        inline ThreadPoolScheduler &Background()
        {
            static ThreadPoolScheduler instance;
            return instance;
        }

        inline TestScheduler &Test()
        {
            static TestScheduler instance;
            return instance;
        }
    }

} // namespace rx

#endif // MICRO_REACTIVE_SCHEDULER_H
