#ifndef MICRO_REACTIVE_PERFORMANCE_H
#define MICRO_REACTIVE_PERFORMANCE_H

#include "core.h"
#include <vector>
#include <queue>
#include <memory>
#include <mutex>
#include <atomic>

namespace rx
{

    // Object pool for memory management optimization
    template <typename T>
    class ObjectPool
    {
    private:
        std::queue<std::unique_ptr<T>> available_objects_;
        std::mutex pool_mutex_;
        std::atomic<size_t> pool_size_;
        std::atomic<size_t> max_size_;
        std::function<std::unique_ptr<T>()> factory_;

    public:
        ObjectPool(std::function<std::unique_ptr<T>()> factory, size_t initial_size = 10, size_t max_size = 100)
            : factory_(factory), pool_size_(0), max_size_(max_size)
        {
            for (size_t i = 0; i < initial_size; ++i)
            {
                Return(factory_());
            }
        }

        std::unique_ptr<T> Acquire()
        {
            std::lock_guard<std::mutex> lock(pool_mutex_);
            if (!available_objects_.empty())
            {
                auto obj = std::move(available_objects_.front());
                available_objects_.pop();
                pool_size_--;
                return obj;
            }
            return factory_();
        }

        void Return(std::unique_ptr<T> obj)
        {
            if (!obj)
                return;

            std::lock_guard<std::mutex> lock(pool_mutex_);
            if (pool_size_.load() < max_size_.load())
            {
                available_objects_.push(std::move(obj));
                pool_size_++;
            }
            // If pool is full, object will be destroyed automatically
        }

        size_t Size() const { return pool_size_.load(); }

        void SetMaxSize(size_t new_max_size) { max_size_.store(new_max_size); }
    };

    // Pooled subscription for better memory management
    class PooledSubscription : public IDisposable
    {
    private:
        std::function<void()> dispose_action_;
        std::atomic<bool> is_disposed_;
        static ObjectPool<PooledSubscription> &GetPool()
        {
            static ObjectPool<PooledSubscription> pool(
                []()
                { return std::unique_ptr<PooledSubscription>(new PooledSubscription()); },
                20, 100);
            return pool;
        }

    public:
        static std::shared_ptr<PooledSubscription> Create(std::function<void()> dispose_action)
        {
            auto pooled = GetPool().Acquire();
            pooled->Initialize(std::move(dispose_action));
            return std::shared_ptr<PooledSubscription>(pooled.release(),
                                                       [](PooledSubscription *p)
                                                       {
                                                           p->Reset();
                                                           auto unique_p = std::unique_ptr<PooledSubscription>(p);
                                                           GetPool().Return(std::move(unique_p));
                                                       });
        }

        void Initialize(std::function<void()> dispose_action)
        {
            dispose_action_ = std::move(dispose_action);
            is_disposed_.store(false);
        }

        void Reset()
        {
            dispose_action_ = nullptr;
            is_disposed_.store(false);
        }

        void Dispose() override
        {
            if (!is_disposed_.exchange(true) && dispose_action_)
            {
                dispose_action_();
            }
        }

        bool IsDisposed() const override
        {
            return is_disposed_.load();
        }
    };

    // Buffer management for large data streams
    template <typename T>
    class CircularBuffer
    {
    private:
        std::vector<T> buffer_;
        size_t capacity_;
        size_t head_;
        size_t tail_;
        size_t size_;
        mutable std::mutex buffer_mutex_;

    public:
        CircularBuffer(size_t capacity)
            : buffer_(capacity), capacity_(capacity), head_(0), tail_(0), size_(0) {}

        bool Push(const T &item)
        {
            std::lock_guard<std::mutex> lock(buffer_mutex_);
            buffer_[tail_] = item;
            tail_ = (tail_ + 1) % capacity_;

            if (size_ < capacity_)
            {
                size_++;
                return true;
            }
            else
            {
                // Buffer is full, overwrite oldest item
                head_ = (head_ + 1) % capacity_;
                return false; // Indicates overflow
            }
        }

        bool Pop(T &item)
        {
            std::lock_guard<std::mutex> lock(buffer_mutex_);
            if (size_ == 0)
            {
                return false;
            }

            item = buffer_[head_];
            head_ = (head_ + 1) % capacity_;
            size_--;
            return true;
        }

        size_t Size() const
        {
            std::lock_guard<std::mutex> lock(buffer_mutex_);
            return size_;
        }

        bool IsEmpty() const
        {
            std::lock_guard<std::mutex> lock(buffer_mutex_);
            return size_ == 0;
        }

        bool IsFull() const
        {
            std::lock_guard<std::mutex> lock(buffer_mutex_);
            return size_ == capacity_;
        }

        void Clear()
        {
            std::lock_guard<std::mutex> lock(buffer_mutex_);
            head_ = tail_ = size_ = 0;
        }
    };

    // Optimized buffer operator using circular buffer
    template <typename T>
    class OptimizedBufferOperator : public Operator<std::vector<T>>
    {
    private:
        std::shared_ptr<IObservable<T>> source_;
        size_t buffer_size_;
        CircularBuffer<T> buffer_;
        std::shared_ptr<Subscription> source_subscription_;
        mutable std::mutex emission_mutex_;

        class BufferObserver : public IObserver<T>
        {
        private:
            std::weak_ptr<OptimizedBufferOperator<T>> parent_;

        public:
            BufferObserver(std::weak_ptr<OptimizedBufferOperator<T>> parent) : parent_(parent) {}

            void OnNext(const T &value) override
            {
                if (auto p = parent_.lock())
                {
                    p->HandleValue(value);
                }
            }

            void OnCompleted() override
            {
                if (auto p = parent_.lock())
                {
                    p->HandleCompleted();
                }
            }

            void OnError(const std::exception &e) override
            {
                if (auto p = parent_.lock())
                {
                    p->NotifyOnError(e);
                }
            }
        };

    public:
        OptimizedBufferOperator(std::shared_ptr<IObservable<T>> source, size_t buffer_size)
            : source_(source), buffer_size_(buffer_size), buffer_(buffer_size) {}

        void HandleValue(const T &value)
        {
            bool should_emit = false;
            {
                std::lock_guard<std::mutex> lock(emission_mutex_);
                buffer_.Push(value);
                should_emit = buffer_.Size() >= buffer_size_;
            }

            if (should_emit)
            {
                EmitBuffer();
            }
        }

        void HandleCompleted()
        {
            EmitBuffer(); // Emit any remaining items
            this->NotifyOnCompleted();
        }

        void EmitBuffer()
        {
            std::vector<T> items;
            items.reserve(buffer_size_);

            {
                std::lock_guard<std::mutex> lock(emission_mutex_);
                T item;
                while (buffer_.Pop(item))
                {
                    items.push_back(std::move(item));
                }
            }

            if (!items.empty())
            {
                this->NotifyOnNext(items);
            }
        }

        std::shared_ptr<Subscription> Subscribe(std::shared_ptr<IObserver<std::vector<T>>> observer) override
        {
            auto base_subscription = Operator<std::vector<T>>::Subscribe(observer);

            if (!source_subscription_)
            {
                auto weak_self = std::weak_ptr<OptimizedBufferOperator<T>>(
                    std::static_pointer_cast<OptimizedBufferOperator<T>>(this->shared_from_this()));
                auto buffer_observer = std::make_shared<BufferObserver>(weak_self);
                source_subscription_ = source_->Subscribe(buffer_observer);
            }

            return base_subscription;
        }
    };

    // Memory monitoring utilities
    class MemoryMonitor
    {
    private:
        static std::atomic<size_t> allocated_bytes_;
        static std::atomic<size_t> peak_bytes_;
        static std::atomic<size_t> allocation_count_;

    public:
        static void RecordAllocation(size_t bytes)
        {
            size_t new_allocated = allocated_bytes_.fetch_add(bytes) + bytes;
            allocation_count_.fetch_add(1);

            // Update peak if necessary
            size_t current_peak = peak_bytes_.load();
            while (new_allocated > current_peak &&
                   !peak_bytes_.compare_exchange_weak(current_peak, new_allocated))
            {
                // Loop until successful update or current_peak is larger
            }
        }

        static void RecordDeallocation(size_t bytes)
        {
            allocated_bytes_.fetch_sub(bytes);
        }

        static size_t GetAllocatedBytes()
        {
            return allocated_bytes_.load();
        }

        static size_t GetPeakBytes()
        {
            return peak_bytes_.load();
        }

        static size_t GetAllocationCount()
        {
            return allocation_count_.load();
        }

        static void Reset()
        {
            allocated_bytes_.store(0);
            peak_bytes_.store(0);
            allocation_count_.store(0);
        }
    };

    // Static initialization
    std::atomic<size_t> MemoryMonitor::allocated_bytes_{0};
    std::atomic<size_t> MemoryMonitor::peak_bytes_{0};
    std::atomic<size_t> MemoryMonitor::allocation_count_{0};

    // RAII memory tracker
    class MemoryTracker
    {
    private:
        size_t tracked_bytes_;

    public:
        MemoryTracker(size_t bytes) : tracked_bytes_(bytes)
        {
            MemoryMonitor::RecordAllocation(bytes);
        }

        ~MemoryTracker()
        {
            MemoryMonitor::RecordDeallocation(tracked_bytes_);
        }

        // Non-copyable, movable
        MemoryTracker(const MemoryTracker &) = delete;
        MemoryTracker &operator=(const MemoryTracker &) = delete;

        MemoryTracker(MemoryTracker &&other) noexcept : tracked_bytes_(other.tracked_bytes_)
        {
            other.tracked_bytes_ = 0;
        }

        MemoryTracker &operator=(MemoryTracker &&other) noexcept
        {
            if (this != &other)
            {
                if (tracked_bytes_ > 0)
                {
                    MemoryMonitor::RecordDeallocation(tracked_bytes_);
                }
                tracked_bytes_ = other.tracked_bytes_;
                other.tracked_bytes_ = 0;
            }
            return *this;
        }
    };

    // Performance-optimized operators
    template <typename T>
    std::shared_ptr<OptimizedBufferOperator<T>> OptimizedBuffer(
        std::shared_ptr<IObservable<T>> source,
        size_t buffer_size)
    {
        return std::make_shared<OptimizedBufferOperator<T>>(source, buffer_size);
    }

    // Utility class for batch processing to reduce overhead
    template <typename T>
    class BatchProcessor
    {
    private:
        std::vector<T> batch_;
        size_t batch_size_;
        std::function<void(const std::vector<T> &)> processor_;
        mutable std::mutex batch_mutex_;

    public:
        BatchProcessor(size_t batch_size, std::function<void(const std::vector<T> &)> processor)
            : batch_size_(batch_size), processor_(processor)
        {
            batch_.reserve(batch_size);
        }

        void Add(const T &item)
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            batch_.push_back(item);

            if (batch_.size() >= batch_size_)
            {
                ProcessBatch();
            }
        }

        void Flush()
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            if (!batch_.empty())
            {
                ProcessBatch();
            }
        }

    private:
        void ProcessBatch()
        {
            if (processor_ && !batch_.empty())
            {
                processor_(batch_);
                batch_.clear();
            }
        }
    };

} // namespace rx

#endif // MICRO_REACTIVE_PERFORMANCE_H
