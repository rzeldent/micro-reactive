// Returns an Observable that emits true if every item emitted by the source Observable satisfies a specified condition, otherwise false

template <typename Tsrc, typename Tdest = bool>
class All : public Operator<Tsrc, Tdest>
{
    class InternalObserver : public IObserver<Tsrc>
    {
    private:
        All<Tsrc, Tdest> *_parent;

    public:
        InternalObserver(All<Tsrc, Tdest> *parent);
        void OnNext(const Tsrc &value);
        void OnComplete();
        void OnError(const std::exception &e);
    };

private:
    InternalObserver _internalObserver;
    IObservable<Tsrc> *_parentObservable;
    std::function<bool(const Tsrc &)> _predicate;

public:
    All(IObservable<Tsrc> *parentObservable, std::function<bool(const Tsrc &)> predicate);
    IObserver<Tdest> *Subscribe(IObserver<Tdest> *observer) override;
    void UnSubscribe(IObserver<Tdest> *observer) override;
};

template <typename Tsrc, typename Tdest>
All<Tsrc, Tdest>::InternalObserver::InternalObserver(All<Tsrc, Tdest> *parent)
    : _parent(parent)
{
}

template <typename Tsrc, typename Tdest>
void All<Tsrc, Tdest>::InternalObserver::OnNext(const Tsrc &value)
{
    if (!_parent->_predicate(value))
        _parent->NotifyOnNext(false);
}

template <typename Tsrc, typename Tdest>
void All<Tsrc, Tdest>::InternalObserver::OnComplete()
{
    _parent->NotifyOnComplete();
}

template <typename Tsrc, typename Tdest>
void All<Tsrc, Tdest>::InternalObserver::OnError(const std::exception &e)
{
    _parent->NotifyOnError(e);
}

template <typename Tsrc, typename Tdest>
All<Tsrc, Tdest>::All(IObservable<Tsrc> *parentObservable, std::function<bool(const Tsrc &)> predicate)
    : _internalObserver(this),
      _parentObservable(parentObservable),
      _predicate(predicate)
{
}

template <typename Tsrc, typename Tdest>
IObserver<Tdest> *All<Tsrc, Tdest>::Subscribe(IObserver<Tdest> *observer)
{
    Observable<Tdest>::Subscribe(observer);
    if (!this->_isComplete && this->_childObservers.size() == 1)
        _parentObservable->Subscribe(&_internalObserver);

    return observer;
}

template <typename Tsrc, typename Tdest>
void All<Tsrc, Tdest>::UnSubscribe(IObserver<Tdest> *observer)
{
    Observable<Tdest>::UnSubscribe(observer);
    if (this->_childObservers.empty())
        _parentObservable->UnSubscribe(&_internalObserver);
}
