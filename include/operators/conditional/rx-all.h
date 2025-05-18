// Returns an Observable that emits true if every item emitted by the source Observable satisfies a specified condition, otherwise false. Emits true if the source Observable terminates without emitting any item

template <typename Tsrc, typename Tdest = bool>
class AllOperator : public Operator<Tdest>
{
    class AllObserver : public IObserver<Tsrc>
    {
    private:
        Operator<Tdest> *_operator;
        std::function<Tdest(const Tsrc &)> _predicate;
        bool _emitted;

    public:
        AllObserver(Operator<Tdest> *op, std::function<Tdest(const Tsrc &)> predicate)
            : _operator(op), _predicate(predicate)
        {
        }

        void OnNext(const Tsrc &value)
        {
            _operator->NotifyOnNext(_predicate(value));
            _emitted = true;
        }

        void OnCompleted()
        {
            if (!_emitted)
            {
                _operator->NotifyOnNext(true);
                _operator->NotifyOnCompleted();
            }
        }

        void OnError(const std::exception &e)
        {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<Tsrc>> _observable;
    std::shared_ptr<AllObserver> _observer;

public:
    AllOperator(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> predicate)
        : _observable(observable)
    {
        _observer = std::make_shared<AllObserver>(this, predicate);
    }

    void Subscribe(std::shared_ptr<IObserver<Tdest>> observer)
    {
        Operator<Tdest>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<Tdest>> observer)
    {
        Operator<Tdest>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename Tsrc, typename Tdest = bool>
std::shared_ptr<AllOperator<Tsrc, Tdest>> All(std::function<Tdest(const Tsrc &)> predicate)
{
    return std::make_shared<AllOperator<Tsrc, Tdest>>(predicate);
}