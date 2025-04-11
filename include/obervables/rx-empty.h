//  Returns an observable that sends no items to observer and immediately completes

template <typename T>
class Empty : public IObservable<T>
{
public:
    void Subscribe(IObserver<T> &observer);
};

template <typename T>
void Empty<T>::Subscribe(IObserver<T> &observer)
{
    observer.OnComplete();
}