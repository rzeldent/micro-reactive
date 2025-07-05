// Returns an observable that executes the specified function when a subscriber subscribes to it
#pragma once

namespace rx 
{
    template <typename T>
    class CreateSource : public IObservable<T>
    {
    public:
        typedef std::function<void(std::shared_ptr<IObserver<T>>)> FactoryType;

    private:
        FactoryType _createFunction;

    public:
        explicit CreateSource(FactoryType createFunction)
            : _createFunction(createFunction)
        {
        }

        void Subscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            if (_createFunction) {
                _createFunction(observer);
            }
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override
        {
            // Default implementation - nothing to do for create observables
        }

        virtual ~CreateSource() = default;
    };

    template <typename T>
    std::shared_ptr<CreateSource<T>> Create(std::function<void(std::shared_ptr<IObserver<T>>)> createFunction)
    {
        return std::make_shared<CreateSource<T>>(createFunction);
    }
}
