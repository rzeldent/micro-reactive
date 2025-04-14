// Emits new items to its subscribers

template <typename T>
class Subject : public IObservable<T>, public IObserver<T>
{
private:
    std::list<IObserver<T>*> _childObservers;

public:
    void Subscribe(IObserver<T>*observer) override;
    void UnSubscribe(IObserver<T>*observer) override;
    void OnNext(T value) override;
    void OnCompleted() override;
    void OnError(const std::exception &e) override;
};

template <typename T>
void Subject<T>::Subscribe(IObserver<T>*observer)
{
    _childObservers.push_back(&observer);
}

template <typename T>
void Subject<T>::UnSubscribe(IObserver<T>*observer)
{
    _childObservers.remove(&observer);
}

template <typename T>
void Subject<T>::OnNext(T value)
{
    for (auto observer : _childObservers)
        observer->OnNext(value);
}

template <typename T>
void Subject<T>::OnCompleted()
{
    for (auto observer : _childObservers)
        observer->OnCompleted();
}

template <typename T>
void Subject<T>::OnError(const std::exception &e)
{
    for (auto observer : _childObservers)
        observer->OnError(e);
}