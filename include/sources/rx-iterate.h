//  Returns an observable that sends each value in the collection

template <typename T>
class IterateObservable : public IObservable<T>
{
public:
    IterateObservable(std::vector<T> values)
        : _values(values)
    {
    }

    void Subscribe(IObserver<T>* observer) override
    {
        for (auto value : _values)
            observer->OnNext(value);

        observer->OnCompleted();
    }

    void UnSubscribe(IObserver<T>* observer) override
    {
    }

    ~IterateObservable() = default;

private:
    std::vector<T> _values;
};

template <typename T>
IterateObservable<T>* Iterate(std::vector<T> values)
{
    return new IterateObservable<T>(values);
}
