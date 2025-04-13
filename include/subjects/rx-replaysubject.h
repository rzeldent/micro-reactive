// Emits new items to its subscribers but also replays a specified subset of its previously emitted items to any new subscribers

template <typename T>
class ReplaySubject : public IObservable<T>, public IObserver<T>
{
private:
    std::list<IObserver<T> *> _childObservers;
    size_t _size;
    std::list<T> _values;

public:
    ReplaySubject(size_t size);
    void Subscribe(IObserver<T> &observer) override;
    void UnSubscribe(IObserver<T> &observer) override;
    void OnNext(T value) override;
    void OnCompleted() override;
    void OnError(const std::exception &e) override;
};

template <typename T>
ReplaySubject<T>::ReplaySubject(size_t size)
    : _size(size)
{
}

template <typename T>
void ReplaySubject<T>::Subscribe(IObserver<T> &observer)
{
    _childObservers.push_back(&observer);
    for (auto value : _values)
        observer.OnNext(value);
}

template <typename T>
void ReplaySubject<T>::UnSubscribe(IObserver<T> &observer)
{
    _childObservers.remove(&observer);
}

template <typename T>
void ReplaySubject<T>::OnNext(T value)
{
    _values.push_back(value);
    if (_values.size() > _size)
        _values.pop_front();

    for (auto observer : _childObservers)
        observer->OnNext(value);
}

template <typename T>
void ReplaySubject<T>::OnCompleted()
{
    for (auto observer : _childObservers)
        observer->OnCompleted();
}

template <typename T>
void ReplaySubject<T>::OnError(const std::exception &e)
{
    for (auto observer : _childObservers)
        observer->OnError(e);
}