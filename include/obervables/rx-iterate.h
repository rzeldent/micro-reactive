//  Returns an observable that sends each value in the collection

template <typename T>
class Iterate : public IObservable<T>
{
public:
    Iterate(std::vector<T> values)
        : _values(values)
    {
    }

    void Subscribe(IObserver<T>* observer) override
    {
        for (auto value : _values)
            this->OnNext(value);

        observer->OnCompleted();
    }

    void UnSubscribe(IObserver<T>* observer) override
    {
    }

private:
    std::vector<T> _values;
    size_t _index = 0;
};

template <typename T>
Iterate<T> IterateObservable(std::vector<T> values)
{
    return *(new Iterate<T>(values));
}
