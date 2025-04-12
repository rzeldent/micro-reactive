template <typename Tsrc, typename Tdest>
class IOperator : public IObservable<Tdest>
{
protected:
	virtual ~IOperator() = default;
};

template <typename Tsrc, typename Tdest>
class Operator : public Observable<Tdest>, IOperator<Tsrc, Tdest>
{
protected:
};