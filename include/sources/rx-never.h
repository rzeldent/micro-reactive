// Returns an observable that never sends any items or notifications to observer

template <typename T>
class NeverObservable : public IObservable<T>
{
public:
    void Subscribe(IObserver<T> *observer)
    {
    }

    void UnSubscribe(IObserver<T> *observer)
    {
    }

    ~NeverObservable() = default;
};

template <typename T>
NeverObservable<T> *Never()
{
    return new NeverObservable<T>();
}