#pragma once

template <typename T>
class IObserver
{
public:
	virtual void OnNext(T value) = 0;
	virtual void OnComplete() = 0;

protected:
	virtual ~IObserver() = default;
};

template <typename T>
class IObservable
{
public:
	virtual void Subscribe(IObserver<T> &observer) = 0;
	virtual void UnSubscribe(IObserver<T> &observer) = 0;

protected:
	virtual ~IObservable() = default;
};

template <typename T>
class IResetable
{
public:
	virtual void Reset() = 0;
	virtual ~IResetable() = default;

protected:
	bool _isComplete = false;
};
