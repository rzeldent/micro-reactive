// Returns an observable that emits an integer at the specified time point

#include <Arduino.h>
template <typename T = unsigned long>
class Timer : public IObservable<T>
{
public:
    Timer(size_t delay)
    {
        // Convert milliseconds to ticks
        _ticks = pdMS_TO_TICKS(delay);
    }

    void Subscribe(IObserver<T> *observer)
    {
        _childObservers.push_back(observer);
        if (_childObservers.size() == 1)
        {
            _timer = xTimerCreate("rx-timer", _ticks, pdFALSE, this, [](TimerHandle_t xTimer)
                                  {
                auto p = static_cast<Timer *>(pvTimerGetTimerID(xTimer));
                auto value = p->_value++;
                for (auto observer : p->_childObservers)
                {
                    observer->OnNext(value);
                    observer->OnCompleted();
                } });

            xTimerStart(_timer, _ticks);
        }
    }

    void UnSubscribe(IObserver<T> *observer)
    {
        _childObservers.remove(observer);
        if (_childObservers.empty() && _timer != nullptr)
        {
            xTimerStop(_timer, 0);
            xTimerDelete(_timer, 0);
            _timer = nullptr;
        }
    }

    ~Timer()
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
Timer<T> *TimerObservable(size_t delay)
{
    return new Timer<T>(delay);
}
