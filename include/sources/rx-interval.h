// Returns an observable that emits a sequential integer every specified time interval
#pragma once
#include <memory>
#include <list>

namespace rx {

template <typename T = unsigned long>
class IntervalObservable : public IObservable<T>
{
public:
    explicit IntervalObservable(size_t interval)
        : _interval(interval), _value(0)
    {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override
    {
        _childObservers.push_back(observer);
        if (_childObservers.size() == 1)
        {
            #ifdef ARDUINO
                // Convert milliseconds to ticks
                _ticks = pdMS_TO_TICKS(_interval);
                _timer = xTimerCreate("rx-interval", _ticks, pdTRUE, this, [](TimerHandle_t xTimer)
                                      {
                    auto p = static_cast<IntervalObservable *>(pvTimerGetTimerID(xTimer));
                    auto value = p->_value++;
                    for (auto observer : p->_childObservers)
                        observer->OnNext(value); });
                xTimerStart(_timer, 0);
            #else
                // Desktop simulation - emit a few values for testing
                for (int i = 0; i < 3; i++) {
                    auto value = _value++;
                    for (auto observer : _childObservers)
                        observer->OnNext(value);
                }
                // Complete after emitting a few values
                for (auto observer : _childObservers)
                    observer->OnCompleted();
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

    ~IntervalObservable()
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
    size_t _interval;
    T _value;
    
    #ifdef ARDUINO
        TickType_t _ticks;
        TimerHandle_t _timer = nullptr;
    #endif
};

template <typename T = unsigned long>
std::shared_ptr<IntervalObservable<T>> Interval(size_t interval)
{
    return std::make_shared<IntervalObservable<T>>(interval);
}

}
