// For each item from only the first of the given observables deliver from the new observable that is returned, on the specified scheduler

template <typename Tsrc, typename Tdest = Tsrc>
class AmbOperator : public Operator<Tdest>
{
    class AmbObserver : public IObserver<Tsrc>
    {
    private:
        Operator<Tdest> *_operator;
        IObservable<Tsrc> **_activeObserver;

    public:
        AmbObserver(Operator<Tdest> *op, IObservable<Tsrc> **_activeObserver)
            : _operator(op), _activeObserver(_activeObserver)
        {
        }

        void OnNext(const Tsrc &value)
        {
            // If there is no active observer this will be the active observer
            if (*_activeObserver == nullptr || *_activeObserver == this)
            {
                *_activeObserver = this;
                _operator->NotifyOnNext(value);
            }
        }

        void OnCompleted()
        {
            if (*_activeObserver == this)
                _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e)
        {
            if (*_activeObserver == this)
                _operator->NotifyOnError(e);
        }
    };

private:
    std::vector<AmbObserver> _observers;
    std::vector<std::shared_ptr<std::shared_ptr<IObservable<Tsrc>>>> _observables;
    AmbObserver * _activeObserver;

public:
    template <typename... Observables>
    AmbOperator(Observables... observables)
        : _observables{observables...}
    {
        _observers = std::vector<std::shared_ptr<AmbObserver>>(sizeof...(observables), &_activeObserver);
    }

    void Subscribe(std::shared_ptr<IObserver<Tdest>> observer)
    {
        Operator<Tdest>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
        {
            for (size_t i = 0; i < _observables.size(); ++i)
                _observables[i]->Subscribe(&_observers[i]);
        }
    }

    void UnSubscribe(std::shared_ptr<IObserver<Tdest>> observer)
    {
        Operator<Tdest>::UnSubscribe(observer);
        if (this->_childObservers.empty())
        {
            for (size_t i = 0; i < _observables.size(); ++i)
                _observables[i]->UnSubscribe(&_observers[i]);
        }
    }
};
