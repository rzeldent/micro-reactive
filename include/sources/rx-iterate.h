//  Returns an observable that sends each value in the collection

template <typename T>
class IterateObservable : public IObservable<T>
{
public:
    IterateObservable(std::vector<T> values)
        : _values(values)
    {
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer)
    {
        for (auto value : _values)
            observer->OnNext(value);

        observer->OnCompleted();
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer)
    {
    }

    ~IterateObservable() = default;

private:
    std::vector<T> _values;
};

template <typename T>
std::shared_ptr<IObservable<T>> Iterate(std::vector<T> values)
{
    return std::shared_ptr<IObservable<T>>(new IterateObservable<T>(values));
}
