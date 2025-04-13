template <typename T>
class Observer : public IObserver<T>
{
public:
    void OnNext(const T &value) 
    {
        // Default implementation does nothing
    }

    void OnCompleted() 
    {
        // Default implementation does nothing
    }

    void OnError(const std::exception &e) 
    {
        // Default implementation does nothing
    }
};