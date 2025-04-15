// Returns an Observable that emits true if any item emitted by the source Observable satisfies a specified condition, otherwise false. Emits false if the source Observable terminates without emitting any item

template <typename Tsrc, typename Tdest = bool>
class Any : public IObserver<Tsrc>, IObservable<Tdest>
{
private:
    std::list<IObserver<Tdest> *> _childObservers;
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
        {
            _emitted = true;
            for (auto observer : _childObservers)
                observer->OnNext(value);
        }
    }

    void OnCompleted()
    {
        for (auto observer : _childObservers)
        {
            if (!_emitted)
                observer->OnNext(false);

            observer->OnCompleted();
        }
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
