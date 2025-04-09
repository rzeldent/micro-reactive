// Returns an observable that emits a sequential integer every specified time interval

template <typename T>
class Interval : public IObservable<T>, public IResetable<T>
{
private:
    std::list<IObserver<T> *> _childObservers;
    unsigned long _interval;
    unsigned long (*_clock)();
    unsigned long _last = 0;
    unsigned long _value = 0;

public:
    Interval(unsigned long interval, T(*clock)());
    void Update();
    void Subscribe(IObserver<T> &observer) override;
    void UnSubscribe(IObserver<T> &observer) override;
    void Reset() override;
};

template <typename T>
Interval<T>::Interval(unsigned long interval, T(*clock)())
    : _interval(interval), _clock(clock)
{
}

template <typename T>
void Interval<T>::Update()
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

template <typename T>
void Interval<T>::Subscribe(IObserver<T> &observer)
{
    _childObservers.push_back(&observer);
}

template <typename T>
void Interval<T>::UnSubscribe(IObserver<T> &observer)
{
    _childObservers.remove(&observer);
}

template <typename T>
void Interval<T>::Reset()
{
    _last = _clock();
    _value = 0;
}
