// Consumes values from an observable using default empty method implementations with optional overrides of each function

template <typename T>
class IObserver
{
public:
	virtual void OnNext(const T &value) = 0;
	virtual void OnCompleted() = 0;
	virtual void OnError(const std::exception &e) = 0;

protected:
	virtual ~IObserver() = default;
};