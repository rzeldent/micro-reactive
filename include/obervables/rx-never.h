// Returns an observable that never sends any items or notifications to observer

template <typename T>
class Never : public IObservable<T>
{
public:
    IObserver<T> *Subscribe(IObserver<T> *observer)
    {
        return observer;
    }

    void UnSubscribe(IObserver<T> *observer)
    {
    }
};

template <typename T>
IObservable<T> *NeverObservable()
{
    return new Never<T>();
}