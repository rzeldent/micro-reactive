// when an item is emitted by either of two Observables, combine the latest item emitted by each Observable via a specified function and emit items based on the results of this function

template <typename Tsrc1, typename Tsrc2, typename Tdest = std::tuple<Tsrc1, Tsrc2>>
class CombineLatest : public IObservable<Tdest>
{
    template <typename T>
    class CombineLatestObserver : public IObserver<T>
    {
    private:
        CombineLatest<Tsrc1, Tsrc2, Tdest> *_parent;
        std::function<void(const T &value)> _fire;

    public:
        CombineLatestObserver(CombineLatest<Tsrc1, Tsrc2, Tdest> *parent, void (CombineLatest::*fire)(const T &value))
            : _parent(parent), _fire([parent, fire](const T &value) { (parent->*fire)(value); })
        {
        }

        void OnNext(const T &value)
        {
            _fire(value);
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
    std::list<std::shared_ptr<IObserver<Tdest>>> _childObservers;
    CombineLatestObserver<Tsrc1> _observer1;
    CombineLatestObserver<Tsrc2> _observer2;

    Tsrc1 _value1;
    Tsrc2 _value2;

    bool _value1Seen = false;
    bool _value2Seen = false;

    IObservable<Tsrc1> *_observable1;
    IObservable<Tsrc2> *_observable2;

public:
    CombineLatest(IObservable<Tsrc1> *observable1, IObservable<Tsrc2> *observable2)
        : _observable1(observable1), _observable2(observable2),
          _observer1(this, &CombineLatest::Fire1), _observer2(this, &CombineLatest::Fire2)
    {
    }

    void Fire1(const Tsrc1 &value)
    {
        _value1 = value;
        _value1Seen = true;
        if (_value2Seen)
            this->NotifyOnNext(std::make_tuple(_value1, _value2));
    }

    void Fire2(const Tsrc2 &value)
    {
        _value2 = value;
        _value2Seen = true;
        if (_value1Seen)
            this->NotifyOnNext(std::make_tuple(_value1, _value2));
    }

    IObserver<Tdest> *Subscribe(IObserver<Tdest> *observer) override
    {
        _childObservers.push_back(observer);
        if (this->_childObservers.size() == 1)
        {
            _observable1->Subscribe(&_observer1);
            _observable2->Subscribe(&_observer2);
        }

        return observer;
    }

    void UnSubscribe(IObserver<Tdest> *observer) override
    {
        _childObservers.remove(observer);
        if (this->_childObservers.empty())
        {
            _observable1->UnSubscribe(&_observer1);
            _observable2->UnSubscribe(&_observer2);
        }
    }
};

template <typename Tsrc1, typename Tsrc2, typename Tdest = std::tuple<Tsrc1, Tsrc2>>
Tdest combineLatest(IObservable<Tsrc1> *observable1, IObservable<Tsrc2> *observable2)
{
    return new CombineLatest<Tsrc1, Tsrc2>(observable1, observable2);
}