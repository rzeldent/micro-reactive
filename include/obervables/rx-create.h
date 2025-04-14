// Returns an observable that executes the specified function when a subscriber subscribes to it

template <typename T>
class Create : public IObservable<T>
{
public:
    typedef std::function<void(IObserver<T>*)> factory;

    Create(factory create)
        : _create(create)
    {
    }

    void Subscribe(IObserver<T>* observer) override
    {
        _create(observer);
    }

    void UnSubscribe(IObserver<T>* observer) override
    {
    }

    ~Create() = default;

    private:
    factory _create;
};

template <typename T>
Create<T>* CreateObservable(typename Create<T>::factory create)
{
    return new Create<T>(create);
}
