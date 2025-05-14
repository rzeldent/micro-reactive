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

    void Subscribe(std::shared_ptr< IObserver<T>> observer)
    {
        for (auto value = _first; value <= _last; value += _step)
            observer->OnNext(value);

        observer->OnCompleted();
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer)
    {
    }

    ~RangeObservable() = default;
};

template <typename T>
std::shared_ptr<IObservable<T>> Range(T first, T last, T step)
{
    return std::shared_ptr<IObservable<T>>(new RangeObservable<T>(first, last, step));
}