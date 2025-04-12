//  Returns an observable that sends no items to observer and immediately completes

template <typename T>
class Empty : public IObservable<T>
{
public:
    IObserver<T> *Subscribe(IObserver<T> *observer)
    {
        observer->OnComplete();
        return observer;
    }

    void UnSubscribe(IObserver<T> *observer)
    {
    }
};

template <typename T>
IObservable<T> *EmptyObservable()
{
    return new Empty<T>();
}