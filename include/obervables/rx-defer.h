// Returns an observable that calls the specified observable factory to create an observable for each new observer that subscribes

template <typename T>
class Defer : public Observable<T>
{
public:
    typedef std::function<IObservable<T>*()> factory;

    Defer<T>(factory create)
        : _create(create)
    {
    }

    void Subscribe(IObserver<T>* observer)
    {
        _childObservers.push_back(observer);
        if (!_observable)
            _observable = std::shared_ptr<IObservable<T>>(_create());

        _observable->Subscribe(observer);
    }

    void UnSubscribe(IObserver<T>* observer)
    {
        _childObservers.remove(observer);
        if (_childObservers.empty())
        {
            _observable->UnSubscribe(observer);
            _observable = nullptr;
        }
    }

    ~Defer()
    {
        if (_observable != nullptr)
            _observable->UnSubscribe(nullptr);

        for (auto observer : _childObservers)
            observer->OnCompleted();

        _childObservers.clear();
    }

private:
    std::list<IObserver<T>*> _childObservers;
    factory _create;
    std::shared_ptr<IObservable<T>> _observable;
};

template <typename T>
Defer<T>* DeferObservable(typename Defer<T>::factory create)
{
    return new Defer<T>(create);
}