// For each item from only the first of the given observables deliver from the new observable that is returned

template <typename Tsrc, typename Tdest = Tsrc>
class Amb : public Operator<Tsrc, Tdest>, Resetable<Tdest>
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
            _parent->Fire(this, value);
        }
        void OnComplete()
        {
            _parent->NotifyOnComplete();
        }
        void OnError(const std::exception &e)
        {
            _parent->NotifyOnError(e);
        }
    };

private:
    AmbObserver _observer1, _observer2;
    IObservable<Tsrc> *_parentObservable1, *_parentObservable2;
    IObserver<Tsrc> *_activeObserver = nullptr;

    void Fire(IObserver<Tsrc> *observer, const Tsrc &value)
    {
        if (_activeObserver == nullptr)
            _activeObserver = observer;

        if (observer == _activeObserver)
            this->NotifyOnNext(value);
    }

public:
    Amb(IObservable<Tsrc> *observable1, IObservable<Tsrc> *observable2)
        : _parentObservable1(observable1), _parentObservable2(observable2), 
          _observer1(this), _observer2(this)
    {
    }
    IObserver<Tdest> *Subscribe(IObserver<Tdest> *observer) override
    {
        Observable<Tdest>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
        {
            _parentObservable1->Subscribe(&_observer1);
            _parentObservable2->Subscribe(&_observer2);
        }

        return observer;
    }
    void UnSubscribe(IObserver<Tdest> *observer) override
    {
        Observable<Tdest>::UnSubscribe(observer);
        if (this->_childObservers.empty())
        {
            _parentObservable1->UnSubscribe(&_observer1);
            _parentObservable2->UnSubscribe(&_observer2);
        }
    }
};
