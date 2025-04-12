//  Returns an observable that sends values in the range [first, last] by adding step to the previous value

template <typename T>
class Range : public Observable<T>, public IResetable<T>
{
private:
    T _first;
    T _last;
    T _step;
    T _value;

    void Fire(IObserver<T> *observer)
    {
        for (auto value = _first; value <= _last; value += _step)
            observer->OnNext(value);
    }

public:
    Range(T first, T last, T step)
        : _first(first), _last(last), _step(step)
    {
    }
    IObserver<T> *Subscribe(IObserver<T> *observer) override
    {
        Observable<T>::Subscribe(observer);
        Fire(observer);
        observer->OnComplete();
        return observer;
    }
    void UnSubscribe(IObserver<T> *observer) override
    {
        Observable<T>::UnSubscribe(observer);
    }
    void Reset() override
    {
        for (auto observer : this->_childObservers)
        {
            Fire(observer);
            observer->OnComplete();
        }
    }
};
