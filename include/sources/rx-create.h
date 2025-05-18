// Returns an observable that executes the specified function when a subscriber subscribes to it

template <typename T>
class CreateObservable : public IObservable<T>
{
public:
    typedef std::function<void(std::shared_ptr<IObserver<T>>)> factory;

    CreateObservable(factory create)
        : _create(create)
    {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer)
    {
        _create(observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer)
    {
    }

    ~CreateObservable() = default;

private:
    factory _create;
};

template <typename T>
std::shared_ptr<CreateObservable<T>> Create(typename CreateObservable<T>::factory create)
{
    return std::make_shared<CreateObservable<T>>(create);
}
