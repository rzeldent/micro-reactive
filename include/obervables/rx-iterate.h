//  Returns an observable that sends each value in the collection

template <typename T>
class Iterate : public IObservable<T>, public IResetable<T>
{
private:
    std::list<IObserver<T> *> _childObservers;
    std::vector<T> _values;
    size_t _index = 0;

public:
    Iterate(std::vector<T> values);
    void Subscribe(IObserver<T> &observer) override;
    void UnSubscribe(IObserver<T> &observer) override;
    void Reset() override;
};

template <typename T>
Iterate<T>::Iterate(std::vector<T> values)
    : _values(values)
{
}

template <typename T>
void Iterate<T>::Subscribe(IObserver<T> &observer)
{
    _childObservers.push_back(&observer);

    for (auto value : _values)
        observer.OnNext(value);

    observer.OnComplete();
}

template <typename T>
void Iterate<T>::UnSubscribe(IObserver<T> &observer)
{
    _childObservers.remove(&observer);
}

template <typename T>
void Iterate<T>::Reset()
{
    for (auto observer : _childObservers)
    {
        for (auto value : _values)
            observer.OnNext(value);

        observer->OnComplete();
    }
}