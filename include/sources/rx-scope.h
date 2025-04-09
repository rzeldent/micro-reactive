// Returns an observable that makes an observable by the specified observable factory using the resource provided by the specified resource factory for each new observer that subscribes

template <typename T>
class Scope : public IObservable<T>, public IResetable<T>
{
private:
    std::vector<T> (*_resourceFactory)();
    IObservable<T> (*_observableFactory)(std::vector<T>);

public:
    Scope(std::vector<T>(*resourceFactory)(), IObservable<T> (*observableFactory)(std::vector<T>));
    void Subscribe(IObserver<T> &observer) override;
};

template <typename T>
Scope<T>::Scope(std::vector<T>(*resourceFactory)(), IObservable<T> (*observableFactory)(std::vector<T>))
    : _resourceFactory(resourceFactory), _observableFactory(observableFactory)
{
}

template <typename T>
void Scope<T>::Subscribe(IObserver<T> &observer)
{
    auto observable = _observableFactory(_resourceFactory());
    observable.Subscribe(observer);
}