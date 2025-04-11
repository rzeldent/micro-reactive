// For each item from only the first of the given observables deliver from the new observable that is returned

template <typename Tsrc, typename Tdest = Tsrc>
class Amb : public IOperator<Tsrc, Tdest>, IResetable<Tdest>
{
private:
    IObservable<Tsrc> *_parentObservable1;
    IObservable<Tsrc> *_parentObservable2;
    bool _disableParentObservable1 = false;
    bool _disableParentObservable2 = false;
    std::list<IObserver<Tdest> *> _childObservers;

public:
    Amb(IObservable<Tsrc> *parentObservable1, IObservable<Tsrc> *parentObservable2);
    void Subscribe(IObserver<Tdest> *observer) override;
    void UnSubscribe(IObserver<Tdest> *observer) override;
    // void OnNext(const Tsrc &value) override;
    // void OnComplete() override;
    // void OnError(const std::exception &e) override;
    // void Reset() override;
    bool _isComplete = false;
};

template <typename Tsrc, typename Tdest>
Amb<Tsrc, Tdest>::Amb(IObservable<Tsrc> *parentObservable1, IObservable<Tsrc> *parentObservable2)
    : _parentObservable1(parentObservable1), _parentObservable2(parentObservable2)
{
}

template <typename Tsrc, typename Tdest>
void Amb<Tsrc, Tdest>::Subscribe(IObserver<Tdest> *observer)
{
    _childObservers.push_back(observer);
    if (!_isComplete && _childObservers.size() == 1)
    {
        _parentObservable1->Subscribe([this](const Tsrc &value)
                                      {
            if (!_disableParentObservable1 && !_isComplete)
            {
                    _disableParentObservable2 = true;
                    for (auto observer : _childObservers)
                        observer->onNext(value);
            } });

        _parentObservable2->Subscribe([this](const Tsrc &value)
                                      {
            if (!_disableParentObservable2 && !_isComplete)
            {
                _disableParentObservable1 = true;
                for (auto observer : _childObservers)
                    observer->onNext(value);
            } });
    }
}

template <typename Tsrc, typename Tdest>
void Amb<Tsrc, Tdest>::UnSubscribe(IObserver<Tdest> *observer)
{
    _childObservers.remove(observer);
    if (_childObservers.empty())
    {
        _parentObservable1->UnSubscribe(this);
        _parentObservable2->UnSubscribe(this);
    }
}
