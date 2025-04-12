// Returns an Observable that emits true if every item emitted by the source Observable satisfies a specified condition, otherwise false. Emits true if the source Observable terminates without emitting any item

template <typename Tsrc, typename Tdest = bool>
class All : public IObserver<Tsrc>, Observable<Tdest>
{
private:
    IObservable<Tsrc> *_observable;
    std::function<bool(const Tsrc &)> _predicate;
    bool _emitted = false;

public:
    All(IObservable<Tsrc> *observable, std::function<bool(const Tsrc &)> predicate)
        : _observable(observable), _predicate(predicate)
    {
    }

    void OnNext(const Tsrc &value) override
    {
        this->NotifyOnNext(_predicate(value));
    }
    void OnComplete()
    {
        if (!_emitted)
            this->NotifyOnNext(true);

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
Tdest all(IObservable<Tsrc> *observable, std::function<bool(const Tsrc &)> predicate)
{
    return new All<Tsrc, Tdest>(observable, predicate);
}