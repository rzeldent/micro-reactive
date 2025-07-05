# Migration Guide: Micro-Reactive Library

This guide helps you migrate from the old complex directory structure to the new simplified structure.

## Quick Migration

### Before (Old Structure)
```cpp
#include "core/observable.h"
#include "sources/rx-range.h"
#include "operators/transform/rx-map.h"
#include "subjects/rx-subject.h"
```

### After (New Structure)
```cpp
#include "micro-reactive.h"
// That's it! Everything is included.
```

## Detailed Changes

### 1. Include Path Changes

| Old Structure | New Structure |
|---------------|---------------|
| `#include "core/observable.h"` | `#include "micro-reactive.h"` |
| `#include "sources/rx-range.h"` | `#include "micro-reactive.h"` |
| `#include "operators/transform/rx-map.h"` | `#include "micro-reactive.h"` |
| `#include "subjects/rx-subject.h"` | `#include "micro-reactive.h"` |

### 2. Directory Structure

**Old Structure (Complex):**
```
include/
├── core/
│   ├── core.h
│   ├── observable.h
│   ├── observer.h
│   ├── operator.h
│   ├── rx-observable.h
│   ├── rx-observer.h
│   └── subject.h
├── sources/
│   ├── rx-create.h
│   ├── rx-range.h
│   ├── rx-empty.h
│   └── ... (9 more files)
├── operators/
│   ├── transform/
│   │   ├── rx-map.h
│   │   ├── rx-filter.h
│   │   └── ... (5 more files)
│   ├── conditional/
│   │   └── ... (5 files)
│   └── combine/
│       └── ... (2 files)
└── subjects/
    └── ... (4 files)
```

**New Structure (Simplified):**
```
include_new/
├── micro-reactive.h    (main include)
├── core.h             (all core interfaces)
├── sources.h          (all source observables)
├── operators.h        (all operators)
└── subjects.h         (all subject types)
```

### 3. Build Configuration Changes

#### PlatformIO
**Old:**
```ini
[env]
build_flags = -Iinclude
```

**New:**
```ini
[env]
build_flags = -Iinclude_new
```

#### CMake
**Old:**
```cmake
target_include_directories(your_target PRIVATE include)
```

**New:**
```cmake
target_include_directories(your_target PRIVATE include_new)
```

#### Arduino IDE
**Old:** Add multiple include paths

**New:** Add single include path: `include_new/`

### 4. Code Examples

#### Example 1: Basic Observable
**Old:**
```cpp
#include "sources/rx-range.h"
#include "operators/transform/rx-map.h"

auto obs = rx::Range(1, 5)
    .Map<int>([](int x) { return x * 2; });
```

**New:**
```cpp
#include "micro-reactive.h"

auto obs = rx::Range(1, 5)
    .Map<int>([](int x) { return x * 2; });
```

#### Example 2: Subject Usage
**Old:**
```cpp
#include "subjects/rx-subject.h"
#include "subjects/rx-behavior.h"

auto subject = rx::Subject<int>();
auto behavior = rx::BehaviorSubject<int>(42);
```

**New:**
```cpp
#include "micro-reactive.h"

auto subject = rx::Subject<int>();
auto behavior = rx::BehaviorSubject<int>(42);
```

### 5. Benefits of Migration

1. **Simpler Includes**: One header file instead of many
2. **Faster Compilation**: Reduced header dependencies
3. **Easier Maintenance**: Flat structure is easier to navigate
4. **Better IDE Support**: Single include path
5. **Reduced Errors**: No more missing include issues

### 6. Migration Steps

1. **Update Includes**: Replace all specific includes with `#include "micro-reactive.h"`
2. **Update Build Scripts**: Change include path from `include` to `include_new`
3. **Test**: Compile and run your existing code
4. **Optional**: Remove old `include/` directory if no longer needed

### 7. Backward Compatibility

The new structure maintains 100% API compatibility. All classes, functions, and namespaces remain the same. Only the include structure has changed.

### 8. Testing Your Migration

Create a simple test file:

```cpp
#include "micro-reactive.h"

int main() {
    // Test basic functionality
    rx::Range(1, 3)
        .Map<int>([](int x) { return x * 2; })
        .Subscribe([](int value) {
            std::cout << "Value: " << value << std::endl;
        });
    
    return 0;
}
```

Compile with:
```bash
g++ -std=c++11 -Iinclude_new test.cpp -o test
```

### 9. Need Help?

If you encounter issues during migration:

1. Check that you're using the correct include path (`include_new/`)
2. Ensure you're only including `micro-reactive.h`
3. Verify your compiler supports C++11
4. Check the examples in the `examples/` directory

The new structure is designed to be drop-in compatible while providing a much cleaner development experience.
