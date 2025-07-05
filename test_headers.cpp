#include "include/micro-reactive.h"
#include <iostream>

int main() 
{
    // Test Create observable
    auto obs = rx::Create<int>([](std::shared_ptr<rx::IObserver<int>> observer) {
        observer->OnNext(1);
        observer->OnNext(2);
        observer->OnCompleted();
    });

    std::cout << "Headers compiled successfully!" << std::endl;
    return 0;
}
