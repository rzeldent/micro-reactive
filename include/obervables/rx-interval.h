// Returns an observable that emits a sequential integer every specified time interval

template <typename T = unsigned long>
class Interval : public Observable<T>, public Resetable<T>
{
private:
    T _interval;
    std::function<T()> _clock;
    T _last = T();
    T _value = T();

public:
    Interval(T interval, std::function<T()> clock)
        : _interval(interval), _clock(clock)
    {
    }
    void Reset() override
    {
        Resetable<T>::Reset();
        _last = _clock();
        _value = T();
    }
    void Update()
    {
        auto current = _clock();
        if (current - _last >= _interval)
        {
            _last = current;
            _value++;
            this->NotifyOnNext(_value);
        }
    }
};