//  Returns an observable that sends no items to observer and immediately completes

template <typename T>
class Empty : public IObservable<T>
{
public:
    IObserver<T> Subscribe(IObserver<T> *observer)
    {
        observer->OnComplete();
        return observer;
    }
};