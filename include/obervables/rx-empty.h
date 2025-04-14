//  Returns an observable that sends no items to observer and immediately completes

template <typename T>
class Empty : public IObservable<T>
{
public:
    void Subscribe(IObserver<T>& observer)
    {
        observer.OnCompleted();
    }

    void UnSubscribe(IObserver<T>& observer)
    {
    }
};

template <typename T>
Empty<T> EmptyObservable()
{
    return *(new Empty<T>());
}
