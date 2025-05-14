//  Returns an observable that sends no items to observer and immediately completes

template <typename T>
class EmptyObservable : public IObservable<T>
{
public:
    void Subscribe(IObserver<T>* observer)
    {
        observer->OnCompleted();
    }

    void UnSubscribe(IObserver<T>* observer)
    {
    }

    ~EmptyObservable() = default;
};

template <typename T>
EmptyObservable<T>* Empty()
{
    return new EmptyObservable<T>();
}
