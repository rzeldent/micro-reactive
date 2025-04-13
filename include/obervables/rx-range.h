//  Returns an observable that sends values in the range [first, last] by adding step to the previous value

template <typename T>
class Range : public IObservable<T>, public IResetable<T>
{
private:
    std::list<IObserver<T>> _childObservers;
    T _first;
    T _last;
    T _step;
    T _value;

public:
    Range(T first, T last, T step)
        : _first(first), _last(last), _step(step)
    {
    }

    void Subscribe(IObserver<T> &observer) override
    {
        _childObservers.push_back(observer);
        for (auto value = _first; value <= _last; value += _step)
            observer->OnNext(value);

        observer->OnCompleted();
    }

    void UnSubscribe(IObserver<T> &observer) override
    {
        _childObservers.remove(observer);
    }

    void Reset() override
    {
        for (auto observer : this->_childObservers)
        {
            for (auto value = _first; value <= _last; value += _step)
                observer->OnNext(value);

            observer->OnCompleted();
        }
    }
};

template <typename T>
IObservable<T> RangeObservable(T first, T last, T step)
{
    return IObservable<T>(new Range<T>(first, last, step));
}