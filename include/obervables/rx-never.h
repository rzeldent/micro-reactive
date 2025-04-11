// Returns an observable that never sends any items or notifications to observer

template <typename T>
class Never : public IObservable<T>
{
public:
    Never();
    void Subscribe(IObserver<T> &observer) override;
};

template <typename T>
Never<T>::Never()
{
}

template <typename T>
void Never<T>::Subscribe(IObserver<T> &observer)
{
    // Do nothing, never sends any items or notifications to observer
}