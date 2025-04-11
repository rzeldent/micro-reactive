// Returns an observable that emits an integer at the specified time point

template <typename T = unsigned long>
class Timer : public IObservable<T>, public IResetable<T>
{
private:
    std::list<IObserver<T> *> _childObservers;
    T _time;
    std::function<T()> _clock;
    T _last = T();
    bool _isComplete = false;

public:
    Timer(unsigned long time, std::function<T()> clock);
    void Update();
    void Subscribe(IObserver<T> &observer) override;
    void UnSubscribe(IObserver<T> &observer) override;
    void Reset() override;
};

template <typename T>
Timer<T>::Timer(T time, std::function<T()> clock)
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
                observer->OnNext(default(T));
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
    _last = T();
    _isComplete = false;
}