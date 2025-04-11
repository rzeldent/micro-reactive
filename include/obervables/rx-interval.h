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
    Interval(T interval, std::function<T()> clock);
    void Reset() override;
    void Update();
};

template <typename T>
Interval<T>::Interval(T interval, std::function<T()> clock)
    : _interval(interval), _clock(clock)
{
}

template <typename T>
void Interval<T>::Update()
{
    if (this->_childObservers.empty())
        return;

    auto current = _clock();
    if (current - _last >= _interval)
    {
        _last = current;
        _value++;
        this->NotifyOnNext(_value);
    }
}

template <typename T>
void Interval<T>::Reset()
{
    Resetable<T>::Reset();
    _last = _clock();
    _value = T();
}
