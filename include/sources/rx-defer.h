// Returns an observable that calls the specified observable factory to create an observable for each new observer that subscribes

template <typename T>
class Defer : public IObservable<T>
{
private:
    IObservable<T> (*_factory)();

 public:
    Defer<T> (IObservable<T> (*factory)());
    void Subscribe(IObserver<T> &observer) override;
};

template <typename T>
Defer<T>::Defer(IObservable<T> (*factory)())
    : _factory(factory)
{
}

template <typename T>
void Defer<T>::Subscribe(IObserver<T> &observer)
{
    auto *observable = _factory();  
    observable->Subscribe(observer);
}
