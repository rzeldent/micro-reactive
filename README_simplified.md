# Micro-Reactive Library (Simplified)

A lightweight, C++11-compatible reactive programming library designed for ESP32/Arduino and embedded systems.

## 📁 Simplified Project Structure

```
micro-reactive/
├── include_new/                    # Simplified headers (NEW)
│   ├── micro-reactive.h           # Main header - include this
│   ├── core.h                     # Core interfaces (IObserver, IObservable, etc.)
│   ├── sources.h                  # All source observables
│   ├── operators.h                # All operators  
│   └── subjects.h                 # All subjects
├── examples/                      # Usage examples
│   ├── main.cpp                   # Basic usage example
│   └── simple_operator_test.cpp   # Operator examples
├── tests/                         # Test suite
│   ├── test_simplified.cpp        # Main test for simplified library
│   ├── comprehensive_test.cpp     # Full source observable tests
│   └── integration_test.cpp       # Complete integration tests
├── include/                       # Original complex structure (legacy)
├── test/                          # Original tests (legacy)
└── src/                           # Original source (legacy)
```

## 🚀 Quick Start

### Include the Library
```cpp
#include "micro-reactive.h"  // Single header includes everything
```

### Basic Usage

#### 1. Source Observables
```cpp
// Range of numbers
auto range = rx::Range(1, 5, 1);  // 1, 2, 3, 4, 5

// Empty observable
auto empty = rx::Empty<int>();

// Never observable (never emits)
auto never = rx::Never<int>();

// Create from function
auto created = rx::Create<int>([](auto observer) {
    observer->OnNext(42);
    observer->OnCompleted();
});
```

#### 2. Operators
```cpp
auto source = rx::Range(1, 10, 1);
std::shared_ptr<rx::IObservable<int>> obs = source;

// Map: transform values
std::function<int(const int&)> mapFunc = [](const int& x) { return x * 2; };
auto mapped = rx::Map<int, int>(obs, mapFunc);

// Filter: conditional filtering
std::function<bool(const int&)> filterFunc = [](const int& x) { return x % 2 == 0; };
auto filtered = rx::Filter(obs, filterFunc);

// Take: limit number of items
auto taken = rx::Take(obs, 3);

// Skip: skip initial items
auto skipped = rx::Skip(obs, 2);
```

#### 3. Subjects
```cpp
// Basic Subject
auto subject = rx::CreateSubject<int>();
subject->Subscribe(observer);
subject->OnNext(42);

// BehaviorSubject (remembers last value)
auto behavior = rx::CreateBehaviorSubject<int>(100);
behavior->Subscribe(observer);  // Immediately gets 100

// ReplaySubject (replays last N values)
auto replay = rx::CreateReplaySubject<int>(5);  // Buffer size 5
```

#### 4. Observer Pattern
```cpp
class MyObserver : public rx::IObserver<int> {
public:
    void OnNext(const int& value) override {
        std::cout << "Value: " << value << std::endl;
    }
    
    void OnCompleted() override {
        std::cout << "Completed" << std::endl;
    }
    
    void OnError(const std::exception& e) override {
        std::cout << "Error: " << e.what() << std::endl;
    }
};

auto observer = std::make_shared<MyObserver>();
observable->Subscribe(observer);
```

## 🔧 Compilation

### Desktop/Testing
```bash
g++ -std=c++11 -I./include_new your_code.cpp -o your_program
```

### PlatformIO/Arduino
Add to your `platformio.ini`:
```ini
lib_deps = 
    # Point to this library's include_new/ directory
build_flags = 
    -std=c++11
```

## 📋 Features

### ✅ Source Observables
- **Empty** - Completes immediately without emitting values
- **Never** - Never emits or completes  
- **Range** - Emits a sequence of numbers
- **Create** - Create from custom function
- **Iterate** - Create from container/array
- **Timer** - Emit after delay
- **Interval** - Emit at intervals
- **Defer** - Lazy creation

### ✅ Operators
- **Map** - Transform each value
- **Filter** - Conditional filtering
- **Take** - Limit number of items
- **Skip** - Skip initial items

### ✅ Subjects
- **Subject** - Basic multicast
- **BehaviorSubject** - Remembers current value
- **ReplaySubject** - Replays last N values
- **SynchronizedSubject** - Thread-safe

## 🏗️ Architecture Benefits

### Simplified Structure
- **4 header files** instead of 20+ nested files
- **Single include** (`micro-reactive.h`) gets everything
- **Flat directory structure** - easy to navigate
- **Consolidated implementation** - easier to maintain

### Original vs Simplified
| Aspect | Original | Simplified |
|--------|----------|------------|
| Header files | 20+ nested | 4 flat |
| Directories | 6 levels deep | 1 level |
| Main include | Complex dependencies | Single header |
| Navigation | Hard to find files | Easy to locate |
| Maintenance | Scattered code | Consolidated |

## 📖 Migration from Complex Structure

If you're using the original complex structure:

### Before (Complex)
```cpp
#include "micro-reactive.h"  // Points to include/micro-reactive.h
```

### After (Simplified)  
```cpp
#include "micro-reactive.h"  // Points to include_new/micro-reactive.h
```

**Change your include path from `./include` to `./include_new`**

## 🧪 Testing

Run the simplified test:
```bash
cd micro-reactive
g++ -std=c++11 -I./include_new tests/test_simplified.cpp -o test_simplified
./test_simplified
```

## 📄 License

See LICENSE file for details.

## 🤝 Contributing

This simplified structure makes contributions easier:
1. **Core interfaces** → `include_new/core.h`
2. **New sources** → Add to `include_new/sources.h`  
3. **New operators** → Add to `include_new/operators.h`
4. **New subjects** → Add to `include_new/subjects.h`
5. **Examples** → Add to `examples/`
6. **Tests** → Add to `tests/`
