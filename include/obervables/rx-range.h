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
    Range(T first, T last, T step)
        : _first(first), _last(last), _step(step)
    {
    }

    IObserver<T> *Subscribe(IObserver<T> *observer) override
    {
        Observable<T>::Subscribe(observer);
        for (auto value = _first; value <= _last; value += _step)
            observer->OnNext(value);

        observer->OnComplete();
        return observer;
    }

    void Reset() override
    {
        for (auto observer : this->_childObservers)
        {
            for (auto value = _first; value <= _last; value += _step)
                observer->OnNext(value);
            observer->OnComplete();
        }
    }
};

template <typename T>
IObservable<T> *RangeObservable(T first, T last, T step)
{
    return new Range<T>(first, last, step);
}