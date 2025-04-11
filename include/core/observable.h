template <typename T>
class IObservable
{
public:
    virtual IObserver<T> *Subscribe(IObserver<T> *observer) = 0;
    virtual void UnSubscribe(IObserver<T> *observer) = 0;

protected:
    virtual ~IObservable() = default;
};

template <typename T>
class Observable : public IObservable<T>
{
protected:
    std::list<IObserver<T> *> _childObservers;
    bool _isComplete = false;

public:
    IObserver<T> *Subscribe(IObserver<T> *observer);
    void UnSubscribe(IObserver<T> *observer);
    void NotifyOnNext(const T &value);
    void NotifyOnComplete();
    void NotifyOnError(const std::exception &e);
};

template <typename T>
IObserver<T> *Observable<T>::Subscribe(IObserver<T> *observer)
{
    _childObservers.push_back(observer);
    return observer;
}

template <typename T>
void Observable<T>::UnSubscribe(IObserver<T> *observer)
{
    _childObservers.remove(observer);
}

template <typename T>
void Observable<T>::NotifyOnNext(const T &value)
{
    for (auto observer : _childObservers)
        observer->OnNext(value);
}

template <typename T>
void Observable<T>::NotifyOnComplete()
{
    for (auto observer : _childObservers)
        observer->OnComplete();
}

template <typename T>
void Observable<T>::NotifyOnError(const std::exception &e)
{
    for (auto observer : _childObservers)
        observer->OnError(e);
}