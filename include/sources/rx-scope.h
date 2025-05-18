// Returns an observable that makes an observable by the specified observable factory using the resource provided by the specified resource factory for each new observer that subscribes

template <typename T>
class ScopeObservable : public IObservable<T>
{
public:
    typedef std::function<std::vector<T>()> resourceFactory;
    typedef std::function<std::shared_ptr<IObservable<T>>(std::vector<T>)> observableFactory;

    ScopeObservable(resourceFactory resourceFactory, observableFactory observableFactory)
        : _resourceFactory(resourceFactory), _observableFactory(observableFactory)
    {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer)
    {
        auto observable = _observableFactory(_resourceFactory());
        _childObservers.push_back(&observer);
        observable.Subscribe(observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer)
    {
    }

    ~ScopeObservable()
    {
        for (auto observer : _childObservers)
            observer->OnCompleted();

        _childObservers.clear();
    }

private:
    std::list<std::shared_ptr<IObserver<T> >> _childObservers;
    std::function<std::vector<T>()> _resourceFactory;
    std::function<std::shared_ptr<IObservable<T>> (std::vector<T>)> _observableFactory;
};

template <typename T>
std::shared_ptr<ScopeObservable<T>> Scope(typename ScopeObservable<T>::resourceFactory resourceFactory, typename ScopeObservable<T>::observableFactory observableFactory)
{
    return std::make_shared<ScopeObservable<T>>(resourceFactory, observableFactory);
}