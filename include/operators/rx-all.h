// Returns an Observable that emits true if every item emitted by the source Observable satisfies a specified condition, otherwise false

template <typename Tsrc, typename Tdest = bool>
class All : public Operator<Tsrc, Tdest>
{
    class AllObserver : public IObserver<Tsrc>
    {
    private:
        All<Tsrc, Tdest> *_parent;

    public:
        AllObserver(All<Tsrc, Tdest> *parent)
            : _parent(parent)
        {
        }
        void OnNext(const Tsrc &value) override
        {
            _parent->NotifyOnNext(_parent->_predicate(value));
        }
        void OnComplete()
        {
            _parent->NotifyOnComplete();
        }
        void OnError(const std::exception &e) override
        {
            _parent->NotifyOnError(e);
        }
    };

private:
    AllObserver _observer;
    IObservable<Tsrc> *_observable;
    std::function<bool(const Tsrc &)> _predicate;

public:
    All(IObservable<Tsrc> *observable, std::function<bool(const Tsrc &)> predicate)
        : _observable(observable), _predicate(predicate),
        _observer(this)
    {
    }
    IObserver<Tdest> *Subscribe(IObserver<Tdest> *observer) override
    {
        Observable<Tdest>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(&_observer);

            return observer;
    }
    void UnSubscribe(IObserver<Tdest> *observer) override
    {
        Observable<Tdest>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(&_observer);
    }
};