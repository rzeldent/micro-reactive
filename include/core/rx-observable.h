template <typename T>
class Observable : public IObservable<T>
{
public:
    void Subscribe(std::shared_ptr<IObserver<T>> observer)
    {
        // Default implementation does nothing
    }

    // Unsubscribe method (not implemented in this example)
    void UnSubscribe(std::shared_ptr<IObserver<T>> observer)
    {
        // Default implementation does nothing
    }
};