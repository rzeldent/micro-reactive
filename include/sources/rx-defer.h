// Returns an observable that calls the specified observable factory to create an observable for each new observer that subscribes

template <typename T>
class DeferObservable : public IObservable<T>
{
public:
    typedef std::function<std::shared_ptr<IObservable<T>>()> factory;

    DeferObservable<T>(factory factory)
        : _factory(factory)
    {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer)
    {
        _childObservers.push_back(observer);
        if (_observable == nullptr)
            _observable = _factory();

        _observable->Subscribe(observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer)
    {
        _childObservers.remove(observer);
        if (_childObservers.empty())
        {
            if (_observable)
            {
                _observable->UnSubscribe(observer);
                _observable.reset();
            }
        }
    }

    ~DeferObservable()
    {
        for (auto observer : _childObservers)
        {
            _observable->UnSubscribe(nullptr);
            observer->OnCompleted();
        }

        _childObservers.clear();

        if (_observable)
            _observable.reset();
    }

private:
    std::list<std::shared_ptr<IObserver<T>>> _childObservers;
    factory _factory;
    std::shared_ptr<IObservable<T>> _observable = nullptr;
};

template <typename T>
std::shared_ptr<DeferObservable<T>> Defer(typename DeferObservable<T>::factory create)
{
    return std::make_shared<DeferObservable<T>>(create);
}