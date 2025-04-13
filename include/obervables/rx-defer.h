// Returns an observable that calls the specified observable factory to create an observable for each new observer that subscribes

template <typename T>
class Defer : public Observable<T>
{
public:
    typedef std::function<IObservable<T>&()> factory;

    Defer<T>(factory create)
        : _create(create)
    {
    }

    void Subscribe(IObserver<T> *observer)
    {
        _observable = _create();
        _observable->Subscribe(observer);
    }

    void UnSubscribe(IObserver<T> *observer)
    {
        _observable->UnSubscribe(observer);
    }

private:
    factory _create;
    IObservable<T> *_observable;
};

template <typename T>
Defer<T> DeferObservable(typename Defer<T>::factory create)
{
    return *(new Defer<T>(create));
}