// Returns an observable that emits a sequential integer every specified time interval

template <typename T = unsigned long>
class Interval : public IObservable<T>, public IResetable<T>
{
private:
    std::list<IObserver<T> *> _childObservers;
    T _interval;
    std::function<T()> _clock;
    T _last = T();
    T _value = T();

public:
    Interval(T interval, std::function<T()> clock)
        : _interval(interval), _clock(clock)
    {
    }

    void Subscribe(IObserver<T> &observer)
    {
        _childObservers.push_back(observer);
    }

    void UnSubscribe(IObserver<T> &observer)
    {
        _childObservers.remove(observer);
    }

    void Reset() override
    {
        _last = _clock();
        _value = T();
    }

    void Update()
    {
        auto current = _clock();
        if (current - _last >= _interval)
        {
            _last = current;
            _value++;
            for (auto observer : _childObservers)
                observer->OnNext(_value);
        }
    }
};

template <typename T>
IObservable<T> IntervalObservable(T interval, std::function<T()> clock)
{
    return new Interval<T>(interval, clock);
}
