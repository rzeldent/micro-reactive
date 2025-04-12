// Returns an observable that calls the specified observable factory to create an observable for each new observer that subscribes

template <typename T>
class Defer : public IObservable<T>
{
private:
    std::function<IObservable<T> *()> _factory;

public:
    Defer<T>(std::function<IObservable<T> *()> factory)
        : _factory(factory)
    {
    }

    IObserver<T> *Subscribe(IObserver<T> *observer)
    {
        auto observable = _factory();
        return observable->Subscribe(observer);
    }

    void UnSubscribe(IObserver<T> *observer)
    {
    }
};

template <typename T>
IObservable<T> *DeferObservable(std::function<IObservable<T> *()> factory)
{
    return new Defer<T>(factory);
}