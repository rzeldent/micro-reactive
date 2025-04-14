template <typename T>
class Observable : public IObservable<T>
{
public:
    void Subscribe(IObserver<T>* observer)
    {
        // Default implementation does nothing
    }

    // Unsubscribe method (not implemented in this example)
    void UnSubscribe(IObserver<T>* observer)
    {
        // Default implementation does nothing
    }
};