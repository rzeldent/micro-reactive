// Returns an observable that executes the specified function when a subscriber subscribes to it

template <typename T>
class Create : public IObservable<T>
{
private:
    std::function<T> _on;

public:
    Create(std::function<T> on);
    void Subscribe(IObserver<T> &observer);
};

template <typename T>
Create<T>::Create(std::function<T> on)
    : _on(on)
{
}

template <typename T>
void Create<T>::Subscribe(IObserver<T> &observer)
{
    observer.OnNext(_on());
}
