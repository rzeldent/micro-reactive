// Returns an observable that emits an integer at the specified time point
#pragma once
#include <memory>
#include <list>

namespace rx {

template <typename T = unsigned long>
class TimerObservable : public IObservable<T>
{
public:
    explicit TimerObservable(size_t delay)
        : _delay(delay), _value(0)
    {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        _childObservers.push_back(observer);
        if (_childObservers.size() == 1)
        {
            #ifdef ARDUINO
                // Convert milliseconds to ticks
                _ticks = pdMS_TO_TICKS(_delay);
                _timer = xTimerCreate("rx-timer", _ticks, pdFALSE, this, [](TimerHandle_t xTimer)
                                      {
                    auto p = static_cast<TimerObservable*>(pvTimerGetTimerID(xTimer));
                    auto value = p->_value++;
                    for (auto observer : p->_childObservers)
                    {
                        observer->OnNext(value);
                        observer->OnCompleted();
                    } });

                xTimerStart(_timer, _ticks);
            #else
                // Desktop simulation - emit immediately for testing
                auto value = _value++;
                for (auto observer : _childObservers)
                {
                    observer->OnNext(value);
                    observer->OnCompleted();
                }
            #endif
        }
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        _childObservers.remove(observer);
        if (_childObservers.empty())
        {
            #ifdef ARDUINO
                if (_timer)
                {
                    xTimerStop(_timer, 0);
                    xTimerDelete(_timer, 0);
                    _timer = nullptr;
                }
            #endif
        }
    }

    ~TimerObservable()
    {
        #ifdef ARDUINO
            if (_timer)
            {
                xTimerStop(_timer, 0);
                xTimerDelete(_timer, 0);
            }
        #endif

        for (auto observer : _childObservers)
            observer->OnCompleted();

        _childObservers.clear();
    }

private:
    std::list<std::shared_ptr<IObserver<T>>> _childObservers;
    size_t _delay;
    T _value;
    
    #ifdef ARDUINO
        TimerHandle_t _timer = nullptr;
        TickType_t _ticks;
    #endif
};

template <typename T = unsigned long>
std::shared_ptr<TimerObservable<T>> Timer(size_t delay)
{
    return std::make_shared<TimerObservable<T>>(delay);
}

}
