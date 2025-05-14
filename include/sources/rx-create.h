// Returns an observable that executes the specified function when a subscriber subscribes to it

template <typename T>
class CreateObservable : public IObservable<T>
{
public:
    typedef std::function<void(IObserver<T> *)> factory;

    CreateObservable(factory create)
        : _create(create)
    {
    }

    void Subscribe(IObserver<T> *observer) override
    {
        _create(observer);
    }

    void UnSubscribe(IObserver<T> *observer) override
    {
    }

    ~CreateObservable() = default;

private:
    factory _create;
};

template <typename T>
CreateObservable<T> *Create(typename CreateObservable<T>::factory create)
{
    return new CreateObservable<T>(create);
}
