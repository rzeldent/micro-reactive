//  Returns an observable that sends each value in the collection

template <typename T>
class Iterate : public Observable<T>, public IResetable<T>
{
private:
    std::vector<T> _values;
    size_t _index = 0;

public:
    Iterate(std::vector<T> values)
        : _values(values)
    {
    }
    IObserver<T> *Subscribe(IObserver<T> *observer) override
    {
        Observable<T>::Subscribe(observer);
        for (auto value : _values)
            this->OnNext(value);

        observer->OnComplete();
        return observer;
    }
    void UnSubscribe(IObserver<T> *observer) override
    {
        Observable<T>::UnSubscribe(observer);
    }
    void Reset() override
    {
        for (auto value : _values)
            this->NotifyOnNext(value);

        this->NotifyOnComplete();
    }
};