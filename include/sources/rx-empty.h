//  Returns an observable that sends no items to observer and immediately completes

template <typename T>
class Empty : public IObservable<T>
{
};