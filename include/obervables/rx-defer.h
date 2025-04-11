// Returns an observable that calls the specified observable factory to create an observable for each new observer that subscribes

template <typename T>
class Defer : public IObservable<T>
{
private:
std::function<IObservable<T>*> _factory;

public:
    Defer<T>(std::function<IObservable<T>*> factory);
    void Subscribe(IObserver<T> &observer);
};

template <typename T>
Defer<T>::Defer(std::function<IObservable<T>*> factory)
    : _factory(factory)
{
}

template <typename T>
void Defer<T>::Subscribe(IObserver<T> &observer)
{
    auto observable = _factory();
    observable->Subscribe(observer);
}
