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

    ~Never() = default;
};

template <typename T>
Never<T> *NeverObservable()
{
    return new Never<T>();
}