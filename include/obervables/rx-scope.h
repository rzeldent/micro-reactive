// Returns an observable that makes an observable by the specified observable factory using the resource provided by the specified resource factory for each new observer that subscribes

template <typename T>
class Scope : public IObservable<T>
{
private:
    std::function<std::vector<T>()> _resourceFactory;
    std::function<IObservable<T>&(std::vector<T>)> _observableFactory;

public:
    Scope(std::function<std::vector<T>()> resourceFactory, std::function<IObservable<T>&(std::vector<T>)> observableFactory)
        : _resourceFactory(resourceFactory), _observableFactory(observableFactory)
    {
    }

    void Subscribe(IObserver<T>& observer)
    {
        auto observable = _observableFactory(_resourceFactory());
        return observable.Subscribe(observer);
    }

    void UnSubscribe(IObserver<T>& observer)
    {
    }
};

template <typename T>
IObservable<T> ScopeObservable(std::function<std::vector<T>()> resourceFactory, std::function<IObservable<T>&(std::vector<T>)> observableFactory)
{
    return IObservable<T>(new Scope<T>(resourceFactory, observableFactory));
}