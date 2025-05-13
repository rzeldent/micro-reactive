// Returns an observable that calls the specified observable factory to create an observable for each new observer that subscribes

template <typename T>
class Defer : public IObservable<T>
{
public:
    typedef std::function<IObservable<T>*()> factory;

    Defer<T>(factory factory)
        : _factory(factory)
    {
    }

    void Subscribe(IObserver<T> *observer)
    {
        _childObservers.push_back(observer);
        if (_observable == nullptr)
            _observable = _factory();

        _observable->Subscribe(observer);
    }

    void UnSubscribe(IObserver<T> *observer)
    {
        _childObservers.remove(observer);
        if (_childObservers.empty())
        {
            if (_observable != nullptr)
            {
                _observable->UnSubscribe(observer);
                delete _observable;
                _observable = nullptr;
            }
        }
    }

    ~Defer()
    {
        for (auto observer : _childObservers)
        {
            _observable->UnSubscribe(nullptr);
            observer->OnCompleted();
        }

        _childObservers.clear();

        if (_observable != nullptr)
            delete _observable;
    }

private:
    std::list<IObserver<T> *> _childObservers;
    factory _factory;
    IObservable<T> *_observable = nullptr;
};

template <typename T>
Defer<T> *DeferObservable(typename Defer<T>::factory create)
{
    return new Defer<T>(create);
}