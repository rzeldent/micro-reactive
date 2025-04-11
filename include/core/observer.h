template <typename T>
class IObserver
{
public:
	virtual void OnNext(const T &value) = 0;
	virtual void OnComplete() = 0;
	virtual void OnError(const std::exception &e) = 0;

protected:
	virtual ~IObserver() = default;
};