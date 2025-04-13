// For each item from only the first of the given observables deliver from the new observable that is returned, on the specified scheduler

template <typename Tsrc, typename Tdest = Tsrc>
class Concat : public IObserver<Tsrc>, IObservable<Tdest>
{
    class ConcatObserver : public IObserver<Tsrc>
    {
    private:
        Concat<Tsrc, Tdest> *_parent;
        std::vector<Tsrc> _buffer;
        bool _isComplete = false;

    public:
        ConcatObserver(Concat<Tsrc, Tdest> *parent)
            : _parent(parent)
        {
        }

        void OnNext(const Tsrc &value)
        {
            if (_parent->_activeObserver == this)
                _parent->NotifyOnNext(value);
            else
                _buffer.push_back(value);
        }

        void OnCompleted()
        {
            _isComplete = true;
            while (_parent->_activeObserver != _parent->_observers.cend())
            {
                // Switch to the next observer
                _parent->_activeObserver++;
                auto observer = _parent->_activeObserver;
                if (!observer->_buffer.empty())
                {
                    // we have some buffered values in the new observer. Call OnNext for each of them
                    for (const auto &value : observer->_buffer)
                        _parent->NotifyOnNext(value);

                    observer->_buffer.clear();
                }

                // Switch to the next observer
                if (!observer->_isComplete)
                    return;
            }

            // No more observers left, notify completion
            _parent->NotifyOnCompleted();
        }

        void OnError(const std::exception &e)
        {
            _parent->NotifyOnError(e);
        }
    };

private:
    std::list<IObserver<Tdest> *> _childObservers;
    std::vector<ConcatObserver> _observers;
    std::vector<IObservable<Tsrc> *> _observables;
    typename std::vector<ConcatObserver>::const_iterator _activeObserver;

public:
    template <typename... Observables>
    Concat(Observables... observables)
        : _observables{observables...}
    {
        _observers.reserve(sizeof...(observables));
        for (size_t i = 0; i < sizeof...(observables); ++i)
            _observers.emplace_back(ConcatObserver(this));

        _activeObserver = _observers.cbegin();
    }

    IObserver<Tdest> *Subscribe(IObserver<Tdest> *observer) override
    {
        _childObservers.push_back(observer);
        if (this->_childObservers.size() == 1)
        {
            for (size_t i = 0; i < _observables.size(); ++i)
                _observables[i]->Subscribe(_observers[i].get());
        }

        return observer;
    }

    void UnSubscribe(IObserver<Tdest> *observer) override
    {
        _childObservers.remove(observer);
        if (this->_childObservers.empty())
        {
            for (size_t i = 0; i < _observables.size(); ++i)
                _observables[i]->UnSubscribe(_observers[i].get());
        }
    }
};
