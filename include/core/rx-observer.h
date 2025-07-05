#pragma once

#include <functional>
#include <memory>
#include <exception>

namespace rx 
{
    template <typename T>
    class Observer : public IObserver<T>
{
public:
    void OnNext(const T &value) override
    {
        // Default implementation does nothing
    }

    void OnCompleted() override
    {
        // Default implementation does nothing
    }

    void OnError(const std::exception &e) override
    {
        // Default implementation does nothing
    }

    virtual ~Observer() = default;
};

// Function-based observer creation
template <typename T>
class FunctionObserver : public IObserver<T>
{
private:
    std::function<void(const T&)> _onNext;
    std::function<void()> _onCompleted;
    std::function<void(const std::exception&)> _onError;

public:
    FunctionObserver(
        std::function<void(const T&)> onNext = nullptr,
        std::function<void()> onCompleted = nullptr,
        std::function<void(const std::exception&)> onError = nullptr)
        : _onNext(onNext), _onCompleted(onCompleted), _onError(onError)
    {
    }

    void OnNext(const T &value) override
    {
        if (_onNext)
            _onNext(value);
    }

    void OnCompleted() override
    {
        if (_onCompleted)
            _onCompleted();
    }

    void OnError(const std::exception &e) override
    {
        if (_onError)
            _onError(e);
    }

    virtual ~FunctionObserver() = default;
};

template <typename T>
std::shared_ptr<FunctionObserver<T>> CreateObserver(
    std::function<void(const T&)> onNext = nullptr,
    std::function<void()> onCompleted = nullptr,
    std::function<void(const std::exception&)> onError = nullptr)
{
    return std::make_shared<FunctionObserver<T>>(onNext, onCompleted, onError);
}
}