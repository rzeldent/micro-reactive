// Returns an observable that emits an integer at the specified time point

template <typename T = unsigned long>
class Timer : public Observable<T>
{
private:
    std::list<IObserver<T> *> _childObservers;
    T _time;
    std::function<T()> _clock;
    T _last = T();

public:
    Timer(T time, std::function<T()> clock)
        : _time(time), _clock(clock)
    {
    }

    void Update()
    {
        if (_childObservers.empty())
            return;

        auto current = _clock();
        if (_last == T())
            _last = current;
        else
        {
            if (current - _last >= _time)
            {
                for (auto observer : _childObservers)
                {
                    observer->OnNext(T());
                    observer->OnCompleted();
                }

                _childObservers.clear();
            }
        }
    }

    ~Timer()
    {
        for (auto observer : _childObservers)
            observer->OnCompleted();

        _childObservers.clear();
    }   
};

template <typename T>
Timer<T> TimerObservable(T time, std::function<T()> clock)
{
    return *(new Timer < T >> (time, clock));
}