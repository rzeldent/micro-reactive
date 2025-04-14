//  Maintains the current value and emits it to any new subscribers, ensuring they receive the most recent data upon subscription

template <typename T>
class BehaviorSubject : public IObservable<T>, public IObserver<T>
{
private:
    std::list<IObserver<T>*> _childObservers;
    T _value;

public:
    BehaviorSubject(T value);
    void Subscribe(IObserver<T>*observer) override;
    void UnSubscribe(IObserver<T>*observer) override;
    void OnNext(const T value) override;
    void OnCompleted() override;
    void OnError(const std::exception &e) override;
};

template <typename T>
BehaviorSubject<T>::BehaviorSubject(T value)
    : _value(value)
{
}

template <typename T>
void BehaviorSubject<T>::Subscribe(IObserver<T>*observer)
{
    _childObservers.push_back(&observer);
    observer.OnNext(_value);
}

template <typename T>
void BehaviorSubject<T>::UnSubscribe(IObserver<T>*observer)
{
    _childObservers.remove(&observer);
}

template <typename T>
void BehaviorSubject<T>::OnNext(const T value)
{
    _value = value;
    for (auto observer : _childObservers)
        observer->OnNext(value);
}

template <typename T>
void BehaviorSubject<T>::OnCompleted()
{
    for (auto observer : _childObservers)
        observer->OnCompleted();
}

template <typename T>
void BehaviorSubject<T>::OnError(const std::exception &e)
{
    for (auto observer : _childObservers)
        observer->OnError(e);
}