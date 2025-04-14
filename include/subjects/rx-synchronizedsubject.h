// A subject that ensures that all notification are delivered to subscribers in a thread-safe manner

template <typename T>
class SynchronizedSubject : public IObservable<T>, public IObserver<T>
{
private:
    Subject<T> _subject;
    std::mutex _mutex;

public:
    void Subscribe(IObserver<T>*observer) override;
    void UnSubscribe(IObserver<T>*observer) override;
    void OnNext(const T value) override;
    void OnCompleted() override;
    void OnError(const std::exception &e) override;
};

template <typename T>
void SynchronizedSubject<T>::Subscribe(IObserver<T>*observer)
{
    std::lock_guard<std::mutex> lock(_mutex);
    _subject.Subscribe(observer);
}

template <typename T>
void SynchronizedSubject<T>::UnSubscribe(IObserver<T>*observer)
{
    std::lock_guard<std::mutex> lock(_mutex);
    _subject.UnSubscribe(observer);
}

template <typename T>
void SynchronizedSubject<T>::OnNext(const T value)
{
    std::lock_guard<std::mutex> lock(_mutex);
    _subject.OnNext(value);
}

template <typename T>
void SynchronizedSubject<T>::OnCompleted()
{
    std::lock_guard<std::mutex> lock(_mutex);
    _subject.OnCompleted();
}

template <typename T>
void SynchronizedSubject<T>::OnError(const std::exception &e)
{
    std::lock_guard<std::mutex> lock(_mutex);
    _subject.OnError(e);
}