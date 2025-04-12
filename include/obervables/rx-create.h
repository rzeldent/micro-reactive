// Returns an observable that executes the specified function when a subscriber subscribes to it

template <typename T>
class Create : public IObservable<T>
{
private:
    std::function<T()> _on;

public:
    Create(std::function<T()> on)
        : _on(on)
    {
    }

    IObserver<T> *Subscribe(IObserver<T> *observer)
    {
        observer->OnNext(_on());
        return observer;
    }

    void UnSubscribe(IObserver<T> *observer)
    {
    }
};

template <typename T>
IObservable<T> *CreateObservable(std::function<T()> on)
{
    return new Create<T>(on);
}