// Returns an observable that emits an integer at the specified time point

template <typename T>
class Timer : public IObservable<T>, public IResetable<T>
{
private:
    std::list<IObserver<T> *> _childObservers;
    unsigned long _time;
    unsigned long (*_clock)();
    unsigned long _last = 0;
    bool _isComplete = false;
public:
    Timer(unsigned long time, T (*clock)());
    void Update();
    void Subscribe(IObserver<T> &observer) override;
    void UnSubscribe(IObserver<T> &observer) override;
    void Reset() override;
};

template <typename T>
Timer<T>::Timer(unsigned long time, T (*clock)())
    : _time(time), _clock(clock)
{
}

template <typename T>
void Timer<T>::Update()
{
    if (_isComplete || _childObservers.empty())
        return;
    
    auto current = _clock();
    if (_last == 0)
        _last = current;
    else
    {
        if (current - _last >= _time)
        {
            _isComplete = true;
            for (auto observer : _childObservers)
            {
                observer->OnNext(1);
                observer->OnComplete();
            }
        }
    }
}

template <typename T>
void Timer<T>::Subscribe(IObserver<T> &observer)
{
    _childObservers.push_back(&observer);
}

template <typename T>
void Timer<T>::UnSubscribe(IObserver<T> &observer)
{
    _childObservers.remove(&observer);
}

template <typename T>
void Timer<T>::Reset()
{
    _last = 0;
    _isComplete = false;
}