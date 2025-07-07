#ifndef MICRO_REACTIVE_H
#define MICRO_REACTIVE_H

/**
 * Micro-Reactive Library
 * A lightweight, C++11-compatible reactive programming library for ESP32/Arduino
 * 
 * Features:
 * - Source Observables (Empty, Never, Range, Create, Iterate, Timer, Interval, Defer)
 * - Transform Operators (Map, Filter, Take, Skip, Debounce, CombineLatest, Merge)
 * - Subjects (Subject, BehaviorSubject, ReplaySubject, SynchronizedSubject)
 * - Error Handling (Catch, Retry, Finally)
 * - Schedulers (Immediate, ThreadPool)
 * - Performance Optimizations (Object pooling, Circular buffers)
 * - Memory-safe shared_ptr based design
 * - Exception handling and error recovery
 * - Thread-safe components with proper resource management
 */

// Core reactive interfaces and base classes
#include "core.h"

// Source observables - create reactive streams
#include "sources.h"

// Transform operators - modify reactive streams (now includes advanced operators)
#include "operators.h"

// Subjects - both observer and observable
#include "subjects.h"

// Error handling operators and utilities
#include "error_handling.h"

// Scheduler interfaces for execution control
#include "scheduler.h"

// Performance optimizations and memory management
#include "performance.h"

#endif // MICRO_REACTIVE_H
