//  Returns an observable that sends values in the range ```first```-```last``` by adding ```step``` to the previous value

template <typename T>
class Range : public IObservable<T>, public IResetable<T>
{
private:
    std::list<IObserver<T> *> _childObservers;
    T _first;
    T _last;
    T _step;
    T _value = 0;
public:
    Range(T first, T last, T step);
    void Subscribe(IObserver<T> &observer) override;
    void UnSubscribe(IObserver<T> &observer) override;
    void Reset() override;
};

template <typename T>
Range<T>::Range(T first, T last, T step)
    : _first(first), _last(last), _step(step)
{
}

template <typename T>
void Range<T>::Subscribe(IObserver<T> &observer)
{
    _childObservers.push_back(&observer);
    _value = _first;
    while (_value <= _last)
    {
        observer.OnNext(_value);
        _value += _step;
    }
 
    observer.OnComplete();
}

template <typename T>
void Range<T>::UnSubscribe(IObserver<T> &observer)
{
    _childObservers.remove(&observer);
}

template <typename T>
void Range<T>::Reset()
{
    for (auto observer : _childObservers)
    {
        _value = _first;
        while (_value <= _last)
        {
            observer->OnNext(_value);
            _value += _step;
        }
 
        observer->OnComplete();
    }
}