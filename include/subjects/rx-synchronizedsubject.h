#ifndef RX_SYNCHRONIZEDSUBJECT_H
#define RX_SYNCHRONIZEDSUBJECT_H

#include <memory>
#include <mutex>
#include <exception>
#include "../core/core.h"
#include "rx-subject.h"

namespace rx {

// A subject that ensures that all notification are delivered to subscribers in a thread-safe manner

template <typename T>
class SynchronizedSubject : public ISubject<T>
{
private:
    std::shared_ptr<Subject<T>> _subject;
    std::mutex _mutex;

public:
    SynchronizedSubject()
        : _subject(CreateSubject<T>())
    {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _subject->Subscribe(observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _subject->UnSubscribe(observer);
    }

    void OnNext(const T &value) override
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _subject->OnNext(value);
    }

    void OnCompleted() override
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _subject->OnCompleted();
    }

    void OnError(const std::exception &e) override
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _subject->OnError(e);
    }

    ~SynchronizedSubject() = default;
};

template <typename T>
std::shared_ptr<SynchronizedSubject<T>> CreateSynchronizedSubject()
{
    return std::make_shared<SynchronizedSubject<T>>();
}

} // namespace rx

#endif // RX_SYNCHRONIZEDSUBJECT_H