// Returns an observable that emits a sequential integer every specified time interval

#include <Arduino.h>
template <typename T = unsigned long>
class IntervalObservable : public IObservable<T>
{
public:
    IntervalObservable(size_t interval)
    {
        // Convert milliseconds to ticks
        _ticks = pdMS_TO_TICKS(interval);
    }

    void Subscribe(IObserver<T> *observer)
    {
        _childObservers.push_back(observer);
        if (_childObservers.size() == 1)
        {
            _timer = xTimerCreate("rx-interval", _ticks, pdTRUE, this, [](TimerHandle_t xTimer)
                                  {
                auto p = static_cast<IntervalObservable *>(pvTimerGetTimerID(xTimer));
                auto value = p->_value++;
                for (auto observer : p->_childObservers)
                    observer->OnNext(value); });
            xTimerStart(_timer, 0);
        }
    }

    void UnSubscribe(IObserver<T> *observer)
    {
        _childObservers.remove(observer);
        if (_childObservers.empty())
        {
            xTimerStop(_timer, 0);
            xTimerDelete(_timer, 0);
            _timer = nullptr;
        }
    }

    ~IntervalObservable()
    {
        if (_timer != nullptr)
        {
            xTimerStop(_timer, 0);
            xTimerDelete(_timer, 0);
        }

        for (auto observer : _childObservers)
            observer->OnCompleted();

        _childObservers.clear();
    }

private:
    std::list<IObserver<T> *> _childObservers;
    TickType_t _ticks;
    xTimerHandle _timer = nullptr;
    T _value = T();
};

template <typename T>
IntervalObservable<T> *Interval(size_t interval)
{
    return new IntervalObservable<T>(interval);
}
