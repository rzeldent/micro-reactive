// Returns an Observable that emits true if any item emitted by the source Observable satisfies a specified condition, otherwise false. Emits false if the source Observable terminates without emitting any item

template <typename Tsrc, typename Tdest = bool>
class Any : public IObserver<Tsrc>, Observable<Tdest>
{
private:
    IObservable<Tsrc> *_observable;
    std::function<bool(const Tsrc &)> _predicate;
    bool _emitted = false;
    bool _completed = false;

public:
    Any(IObservable<Tsrc> *observable, std::function<bool(const Tsrc &)> predicate)
        : _observable(observable), _predicate(predicate)
    {
    }

    void OnNext(const Tsrc &value) override
    {
        if (!_emitted && _predicate(value))
            this->NotifyOnNext(value);
    }

    void OnComplete()
    {
        if (!_emitted)
            this->NotifyOnNext(false);

        this->NotifyOnComplete();
    }

    void OnError(const std::exception &e) override
    {
        this->NotifyOnError(e);
    }

    IObserver<Tdest> *Subscribe(IObserver<Tdest> *observer) override
    {
        Observable<Tdest>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(this);

        return observer;
    }

    void UnSubscribe(IObserver<Tdest> *observer) override
    {
        Observable<Tdest>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(this);
    }
};

template <typename Tsrc, typename Tdest>
Tdest any(IObservable<Tsrc> *observable, std::function<bool(const Tsrc &)> predicate)
{
    return new Any<Tsrc, Tdest>(observable, predicate);
}