// Returns an observable that executes the specified function when a subscriber subscribes to it

template <typename T>
class Create : public IObservable<T>
{
private:
    std::function<void(IObserver<T> &)> _onCreate;

public:
    Create(std::function<void(IObserver<T> &)> onCreate)
        : _onCreate(onCreate)
    {
    }

    void Subscribe(IObserver<T> &observer) override
    {
        _onCreate(observer);
    }

    void UnSubscribe(IObserver<T> &observer) override
    {
        // Unsubscribe logic can be implemented here if needed
    }
};

// template <typename T>
// Create<T> CreateObservable(std::function<Observer<T>()> onCreate)
// {
//     return *(new Create<T>(onCreate));
// }
