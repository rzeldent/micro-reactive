#pragma once

namespace rx 
{
    template <typename T>
    class ISubject : public IObservable<T>, IObserver<T>
    {
    protected:
        virtual ~ISubject() = default;
    };
}