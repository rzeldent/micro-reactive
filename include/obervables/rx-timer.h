// Returns an observable that emits an integer at the specified time point

template <typename T = unsigned long>
class Timer : public Observable<T>, public IResetable<T>
{
private:
    T _time;
    std::function<T()> _clock;
    T _last = T();

public:
    Timer(T time, std::function<T()> clock)
        : _time(time), _clock(clock)
    {
    }

    void Reset() override
    {
        _last = T();
        _isComplete = false;
    }

    void Update()
    {
        if (this->_isComplete)
            return;

        auto current = _clock();
        if (_last == T())
            _last = current;
        else
        {
            if (current - _last >= _time)
            {
                _isComplete = true;
                this->NotifyOnNext(T());
                this->NotifyOnCompleted();
            }
        }
    }
};

template <typename T>
IObservable<T>& TimerObservable(T time, std::function<T()> clock)
{
    return std::shared_ptr<IObservable>(new Timer<T>>(time, clock));
}