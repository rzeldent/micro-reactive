#include <iostream>
#include <cassert>
#include <stdexcept>

// Simple Unity-like test framework for compilation testing
#define TEST_ASSERT_EQUAL(expected, actual) \
    do { \
        if ((expected) != (actual)) { \
            std::cerr << "TEST FAILED: " << __FILE__ << ":" << __LINE__ \
                      << " Expected: " << (expected) << ", Actual: " << (actual) << std::endl; \
            throw std::runtime_error("Test assertion failed"); \
        } \
    } while(0)

#define TEST_ASSERT_TRUE(condition) \
    do { \
        if (!(condition)) { \
            std::cerr << "TEST FAILED: " << __FILE__ << ":" << __LINE__ \
                      << " Expected true, got false" << std::endl; \
            throw std::runtime_error("Test assertion failed"); \
        } \
    } while(0)

#define TEST_ASSERT_FALSE(condition) \
    do { \
        if (condition) { \
            std::cerr << "TEST FAILED: " << __FILE__ << ":" << __LINE__ \
                      << " Expected false, got true" << std::endl; \
            throw std::runtime_error("Test assertion failed"); \
        } \
    } while(0)

#define TEST_ASSERT_EQUAL_STRING(expected, actual) \
    do { \
        if (std::string(expected) != std::string(actual)) { \
            std::cerr << "TEST FAILED: " << __FILE__ << ":" << __LINE__ \
                      << " Expected: \"" << (expected) << "\", Actual: \"" << (actual) << "\"" << std::endl; \
            throw std::runtime_error("Test assertion failed"); \
        } \
    } while(0)

#define RUN_TEST(test_func) \
    do { \
        try { \
            std::cout << "Running " << #test_func << "..." << std::endl; \
            test_func(); \
            std::cout << "  PASSED" << std::endl; \
            tests_passed++; \
        } catch (const std::exception& e) { \
            std::cout << "  FAILED: " << e.what() << std::endl; \
            tests_failed++; \
        } \
        tests_total++; \
    } while(0)

#define UNITY_BEGIN() \
    do { \
        std::cout << "Starting tests..." << std::endl; \
    } while(0)

#define UNITY_END() \
    do { \
        std::cout << "\nTest Results:" << std::endl; \
        std::cout << "Tests run: " << tests_total << std::endl; \
        std::cout << "Tests passed: " << tests_passed << std::endl; \
        std::cout << "Tests failed: " << tests_failed << std::endl; \
        if (tests_failed == 0) { \
            std::cout << "ALL TESTS PASSED!" << std::endl; \
        } else { \
            std::cout << "SOME TESTS FAILED!" << std::endl; \
        } \
    } while(0)

// Global test counters
int tests_total = 0;
int tests_passed = 0;
int tests_failed = 0;

// Mock Arduino functions for testing
void delay(int ms) {
    // No-op for testing
}

#include "../include/micro-reactive.h"

// Include test files
#include "test/test_operators.cpp"
#include "test/test_subjects.cpp"
#include "test/test_sources.cpp"
#include "test/test_integration.cpp"

void setUp(void) {
    // Set up code here - runs before each test
}

void tearDown(void) {
    // Clean up code here - runs after each test
}

// Test runner for compilation testing
int main() {
    UNITY_BEGIN();
    
    // Run all test suites
    std::cout << "\n=== Testing Operators ===" << std::endl;
    test_operators_suite();
    
    std::cout << "\n=== Testing Subjects ===" << std::endl;
    test_subjects_suite();
    
    std::cout << "\n=== Testing Sources ===" << std::endl;
    test_sources_suite();
    
    std::cout << "\n=== Testing Integration ===" << std::endl;
    test_integration_suite();
    
    UNITY_END();
    
    return (tests_failed == 0) ? 0 : 1;
}
