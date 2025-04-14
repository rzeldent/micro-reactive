// Returns an observable that emits a sequential integer every specified time interval

template <typename T = unsigned long>
class Interval : public IObservable<T>
{
public:
    Interval(T interval, std::function<T()> clock)
        : _interval(interval), _clock(clock)
    {
    }

    void Subscribe(IObserver<T>* observer)
    {
        _childObservers.push_back(observer);
    }

    void UnSubscribe(IObserver<T>* observer)
    {
        _childObservers.remove(observer);
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

    ~Interval()
    {
        for (auto observer : _childObservers)
            observer->OnCompleted();

        _childObservers.clear();
    }

private:
    std::list<IObserver<T>*> _childObservers;
    T _interval;
    std::function<T()> _clock;
    T _last = T();
    T _value = T();
};

template <typename T>
Interval<T> IntervalObservable(T interval, std::function<T()> clock)
{
    return *(new Interval<T>(interval, clock));
}
