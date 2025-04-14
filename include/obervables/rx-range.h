//  Returns an observable that sends values in the range [first, last] by adding step to the previous value

template <typename T>
class Range : public IObservable<T>
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

    void Subscribe(IObserver<T>*observer) override
    {
        for (auto value = _first; value <= _last; value += _step)
            observer->OnNext(value);

        observer->OnCompleted();
    }

    void UnSubscribe(IObserver<T>*observer) override
    {
    }

    ~Range() = default;
};

template <typename T>
Range<T> RangeObservable(T first, T last, T step)
{
    return *(new Range<T>(first, last, step));
}