// Return an observable that emits connected, non-overlapping buffer, each containing at most count items from the source observable.
// If the skip parameter is set, return an observable that emits buffers every skip items containing at most count items from the source observable.

template <typename Tsrc, typename Tdest = std::vector<Tsrc>>
class BufferCount : public IObserver<Tsrc>, IObservable<Tdest>
{
private:
    std::list<IObserver<Tdest> *> _childObservers;
    IObservable<Tsrc> *_observable;
    Tdest _buffer;
    size_t _count;

public:
    BufferCount(IObservable<Tsrc> *observable, size_t count)
        : _observable(observable), _count(count)
    {
    }

    void OnNext(const Tsrc &value) override
    {
        _buffer.push_back(value);
        if (_buffer.size() == _count)
        {
            for (auto observer : _childObservers)
                observer->OnNext(_buffer);

            _buffer.clear();
        }
    }

    void OnCompleted() override
    {
        if (_buffer.size() > 0)
        {
            for (auto observer : _childObservers)
                observer->OnNext(_buffer);

            _buffer.clear();
        }

        this->NotifyOnCompleted();
    }

    void OnError(const std::exception &e) override
    {
        for (auto observer : _childObservers)
            observer->OnError(e);
    }

    IObserver<Tdest> *Subscribe(IObserver<Tdest> *observer) override
    {
        _childObservers.push_back(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(this);

        return observer;
    }

    void UnSubscribe(IObserver<Tdest> *observer) override
    {
        _childObservers.remove(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(this);
    }
};

template <typename Tsrc, typename Tdest>
Tdest bufferCount(IObservable<Tsrc> *observable, size_t count)
{
    return new BufferCount<Tsrc, Tdest>(observable, count);
}