//  Returns an observable that sends each value in the collection

template <typename T>
class Iterate : public IObservable<T>, public IResetable<T>
{
private:
    std::list<IObserver<T> *> _childObservers;
    std::vector<T> _values;
    size_t _index = 0;

public:
    Iterate(std::vector<T> values)
        : _values(values)
    {
    }

    void Subscribe(IObserver<T> &observer) override
    {
        _childObservers.push_back(observer);
        for (auto value : _values)
            this->OnNext(value);

        observer->OnCompleted();
    }

    void UnSubscribe(IObserver<T> &observer) override
    {
        _childObservers.remove(observer);
    }

    void Reset() override
    {
        for (auto value : _values)
            for (auto observer : _childObservers)
            {
                observer->OnNext(value);
                observer->OnCompleted();
            }
    }
};

template <typename T>
IObservable<T> IterateObservable(std::vector<T> values)
{
    return IObservable<T>(new Iterate<T>(values));
}