//  Returns an observable that sends values in the range [first, last] by adding step to the previous value

template <typename T>
class RangeObservable : public IObservable<T>
{
private:
    T _first;
    T _last;
    T _step;
    T _value;

public:
    RangeObservable(T first, T last, T step)
        : _first(first), _last(last), _step(step)
    {
    }

    void Subscribe(IObserver<T> *observer) override
    {
        for (auto value = _first; value <= _last; value += _step)
            observer->OnNext(value);

        observer->OnCompleted();
    }

    void UnSubscribe(IObserver<T> *observer) override
    {
    }

    ~RangeObservable() = default;
};

template <typename T>
RangeObservable<T> *Range(T first, T last, T step)
{
    return new RangeObservable<T>(first, last, step);
}