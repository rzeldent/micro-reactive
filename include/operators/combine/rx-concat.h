// For each item from only the first of the given observables deliver from the new observable that is returned, on the specified scheduler

template <typename Tsrc, typename Tdest = Tsrc>
class Concat : public Observable<Tdest>
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

        void OnComplete()
        {
            _isComplete = true;
            while (_parent->_activeObserver != _parent->_observers.cend())
            {
                // Use next observer
                auto observer = (_parent->_activeObserver++).get();
                if (!observer->_buffer.empty())
                {
                    // we have some buffered values, notify them
                    for (const auto &value : _buffer)
                        _parent->NotifyOnNext(value);

                    _buffer.clear();
                }

                if (!observer->_isComplete)
                    return;
            }

            // No more observers left, notify completion
            _parent->NotifyOnComplete();
        }

        void OnError(const std::exception &e)
        {
            _parent->NotifyOnError(e);
        }
    };

private:
    std::vector<std::unique_ptr<ConcatObserver>> _observers;
    std::vector<IObservable<Tsrc> *> _observables;
    typename std::vector<std::unique_ptr<ConcatObserver>>::const_iterator _activeObserver = _observers.cbegin();

public:
    template <typename... Observables>
    Concat(Observables... observables)
        : _observables{observables...}
    {
        _observers.reserve(sizeof...(observables));
        for (size_t i = 0; i < sizeof...(observables); ++i)
        {
            _observers.emplace_back(std::make_unique<ConcatObserver>(this));
        }
    }

    IObserver<Tdest> *Subscribe(IObserver<Tdest> *observer) override
    {
                _observables[i]->Subscribe(_observers[i].get());
        if (this->_childObservers.size() == 1)
        {
            for (size_t i = 0; i < _observables.size(); ++i)
                _observables[i]->Subscribe(_observers[i].get());
        }

        return observer;
    }

    void UnSubscribe(IObserver<Tdest> *observer) override
    {
                _observables[i]->UnSubscribe(_observers[i].get());
        if (this->_childObservers.empty())
        {
            for (size_t i = 0; i < _observables.size(); ++i)
                _observables[i]->UnSubscribe(_observers[i].get());
        }
    }
};
