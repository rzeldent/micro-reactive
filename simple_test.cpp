#include <iostream>
#include <memory>
#include <functional>

// Simple test version without Arduino.h
namespace rx {
    template<typename T>
    class IObserver {
    public:
        virtual void OnNext(const T& value) = 0;
        virtual void OnCompleted() = 0;
        virtual void OnError(const std::exception& e) = 0;
        virtual ~IObserver() = default;
    };

    template<typename T>
    class IObservable {
    public:
        virtual void Subscribe(std::shared_ptr<IObserver<T>> observer) = 0;
        virtual void UnSubscribe(std::shared_ptr<IObserver<T>> observer) = 0;
        virtual ~IObservable() = default;
    };

    template<typename T>
    class CreateSource : public IObservable<T> {
    public:
        using factory = std::function<void(std::shared_ptr<IObserver<T>>)>;

    private:
        factory _factory;

    public:
        CreateSource(factory factory) : _factory(factory) {}

        void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
            try {
                _factory(observer);
            }
            catch(const std::exception& e) {
                observer->OnError(e);
            }
        }

        void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
            // Default implementation - nothing to do for create observables
        }
    };

    template<typename T>
    std::shared_ptr<CreateSource<T>> Create(std::function<void(std::shared_ptr<IObserver<T>>)> createFunction) {
        return std::make_shared<CreateSource<T>>(createFunction);
    }
}

int main() {
    // Test Create observable
    auto obs = rx::Create<int>([](std::shared_ptr<rx::IObserver<int>> observer) {
        observer->OnNext(1);
        observer->OnNext(2);
        observer->OnCompleted();
    });

    std::cout << "Simple test compiled successfully!" << std::endl;
    return 0;
}
