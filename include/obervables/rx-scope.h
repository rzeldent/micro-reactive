// Returns an observable that makes an observable by the specified observable factory using the resource provided by the specified resource factory for each new observer that subscribes

template <typename T>
class Scope : public IObservable<T>
{
public:
    typedef std::function<std::vector<T>()> resourceFactory;
    typedef std::function<std::shared_ptr<IObservable<T>>(std::vector<T>)> observableFactory;

    Scope(resourceFactory resourceFactory, observableFactory observableFactory)
        : _resourceFactory(resourceFactory), _observableFactory(observableFactory)
    {
    }

    void Subscribe(IObserver<T> &observer)
    {
        auto observable = _observableFactory(_resourceFactory());
        _childObservers.push_back(&observer);
        observable.Subscribe(observer);
    }

    void UnSubscribe(IObserver<T> &observer)
    {
    }

    ~Scope()
    {
        for (auto observer : _childObservers)
            observer->OnCompleted();

        _childObservers.clear();
    }

private:
    std::list<IObserver<T> *> _childObservers;
    std::function<std::vector<T>()> _resourceFactory;
    std::function<IObservable<T> &(std::vector<T>)> _observableFactory;
};

template <typename T>
Scope<T> ScopeObservable(typename Scope<T>::resourceFactory resourceFactory, typename Scope<T>::observableFactory observableFactory)
{
    return *(new Scope<T>(resourceFactory, observableFactory));
}