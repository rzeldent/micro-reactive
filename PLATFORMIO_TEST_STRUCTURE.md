# PlatformIO Test Structure Documentation

## Overview
The micro-reactive library has been reorganized to be fully compatible with PlatformIO's testing framework. All tests are now located in the `test/` directory with a proper structure.

## Test Structure

```
test/
├── main.cpp                 # Main test runner for PlatformIO
├── test_operators.cpp       # Tests for all operators
├── test_subjects.cpp        # Tests for Subject and BehaviorSubject
├── test_sources.cpp         # Tests for Range, FromVector, Create, etc.
└── test_integration.cpp     # Integration tests with operator chaining
```

## Running Tests

### With PlatformIO
```bash
# Run tests on native environment (computer)
pio test --environment native

# Run tests on ESP32 hardware
pio test --environment esp32-c3-devkitm-1

# Run specific test
pio test --filter test_operators
```

### With Regular C++ Compiler (for development)
```bash
# Compile and run test runner
g++ -std=c++11 -I./include test_runner.cpp -o test_runner
./test_runner
```

## Test Organization

### test_operators.cpp
Contains tests for all operators:
- Basic operators: Map, Filter, Take, Skip
- Advanced operators: Distinct, Scan, Reduce, First, Last
- New operators: Buffer, TakeWhile, SkipWhile, StartWith, DefaultIfEmpty
- Aggregation operators: Count, Sum, Min, Max
- LINQ aliases: Where, Select
- Operator chaining

### test_subjects.cpp
Contains tests for reactive subjects:
- Basic Subject: subscription, unsubscription, multicasting
- BehaviorSubject: initial value, late subscribers, current value
- Error handling and propagation
- Using subjects with operators

### test_sources.cpp
Contains tests for observable sources:
- Range: with different parameters and steps
- FromVector: various data types and edge cases
- Iterate: container iteration
- Create: custom observable creation
- Error handling in sources

### test_integration.cpp
Contains comprehensive integration tests:
- Complex operator chains
- Real-world usage scenarios
- Error propagation through chains
- Multiple observers
- Performance and memory considerations

## Test Features

### Unity Framework Compatible
- Uses Unity test framework assertions
- Proper test organization and reporting
- Setup/teardown functions for each test
- Detailed test results and error reporting

### C++11 Compliance
- All tests work with C++11 standard
- Compatible with Arduino/ESP32 toolchain
- No modern C++ features that might not be available

### Comprehensive Coverage
- Tests all operators and functionality
- Edge cases and error conditions
- Memory management verification
- Operator chaining validation

### PlatformIO Integration
- Follows PlatformIO test conventions
- Configurable for different environments
- Can run on both native and embedded hardware
- Proper build configuration in platformio.ini

## Configuration

The `platformio.ini` file has been updated with test configuration:

```ini
; Test configuration
test_framework = unity
test_filter = test_*

[env]
platform = espressif32
framework = arduino
monitor_speed = 115200
```

## Notes for Developers

1. **Adding New Tests**: Add new test functions to the appropriate test file and include them in the test suite function.

2. **Test Naming**: Follow the convention `test_[functionality]_[specific_case]` for test function names.

3. **Assertions**: Use Unity framework assertions (TEST_ASSERT_EQUAL, TEST_ASSERT_TRUE, etc.).

4. **Test Data**: Keep test data simple and focused on the specific functionality being tested.

5. **Error Testing**: Include tests for error conditions and edge cases.

6. **Memory Management**: Ensure proper cleanup in tests to avoid memory leaks.

## Benefits of This Structure

- **PlatformIO Native**: Fully compatible with PlatformIO testing
- **Organized**: Tests grouped by functionality for easy maintenance
- **Comprehensive**: Covers all library features with edge cases
- **Portable**: Can run on both development machine and target hardware
- **Maintainable**: Clear structure makes adding new tests easy
- **Professional**: Follows industry standards for embedded testing
