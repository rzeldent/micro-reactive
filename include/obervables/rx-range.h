//  Returns an observable that sends values in the range [first, last] by adding step to the previous value

template <typename T>
class Range : public Observable<T>, public IResetable<T>
{
private:
    T _first;
    T _last;
    T _step;
    T _value;

public:
    Range(T first, T last, T step);
    IObserver<T> *Subscribe(IObserver<T> *observer) override;
    void UnSubscribe(IObserver<T> *observer) override;
    void Reset() override;
};

template <typename T>
Range<T>::Range(T first, T last, T step)
    : _first(first), _last(last), _step(step)
{
}

template <typename T>
IObserver<T> *Range<T>::Subscribe(IObserver<T> *observer)
{
    Observable<T>::Subscribe(observer);
    _value = _first;
    while (_value <= _last)
    {
        observer->OnNext(_value);
        _value += _step;
    }

    observer->OnComplete();
    return observer;
}

template <typename T>
void Range<T>::UnSubscribe(IObserver<T> *observer)
{
    Observable<T>::UnSubscribe(observer);
}

template <typename T>
void Range<T>::Reset()
{
    for (auto observer : this->_childObservers)
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