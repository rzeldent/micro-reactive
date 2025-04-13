// For each item from only the first of the given observables deliver from the new observable that is returned, on the specified scheduler

template <typename Tsrc, typename Tdest = Tsrc>
class Amb : public IObserver<Tsrc>, IObservable<Tdest>
{
    class AmbObserver : public IObserver<Tsrc>
    {
    private:
        Amb<Tsrc, Tdest> *_parent;

    public:
        AmbObserver(Amb<Tsrc, Tdest> *parent)
            : _parent(parent)
        {
        }

        void OnNext(const Tsrc &value)
        {
            // If there is no active observer this will be the active observer
            if (_parent->_activeObserver == nullptr || _parent->_activeObserver == this)
            {
                _parent->_activeObserver = this;
                _parent->NotifyOnNext(value);
            }
        }

        void OnCompleted()
        {
            _parent->NotifyOnCompleted();
        }

        void OnError(const std::exception &e)
        {
            _parent->NotifyOnError(e);
        }
    };

private:
    std::list<IObserver<Tdest> *> _childObservers;
    std::vector<AmbObserver> _observers;
    std::vector<IObservable<Tsrc> *> _observables;
    AmbObserver *_activeObserver = nullptr;

public:
    template <typename... Observables>
    Amb(Observables... observables)
        : _observables{observables...}
    {
        _observers = std::vector<AmbObserver>(sizeof...(observables), AmbObserver(this));
    }

    IObserver<Tdest> *Subscribe(IObserver<Tdest> *observer) override
    {
        _childObservers.push_back(observer);
        if (this->_childObservers.size() == 1)
        {
            for (size_t i = 0; i < _observables.size(); ++i)
                _observables[i]->Subscribe(&_observers[i]);
        }

        return observer;
    }

    void UnSubscribe(IObserver<Tdest> *observer) override
    {
        _childObservers.remove(observer);
        if (this->_childObservers.empty())
        {
            for (size_t i = 0; i < _observables.size(); ++i)
                _observables[i]->UnSubscribe(&_observers[i]);
        }
    }
};

template <typename Tsrc, typename Tdest = Tsrc, typename... Observables>
Amb<Tsrc, Tdest> *amb(Observables... observables)
{
    return new Amb<Tsrc, Tdest>(observables...);
}
