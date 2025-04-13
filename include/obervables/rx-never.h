// Returns an observable that never sends any items or notifications to observer

template <typename T>
class Never : public IObservable<T>
{
public:
    void Subscribe(IObserver<T> *observer)
    {
    }

    void UnSubscribe(IObserver<T> *observer)
    {
    }
};

template <typename T>
IObservable<T> NeverObservable()
{
    return IObservable<T>(new Never<T>());
}