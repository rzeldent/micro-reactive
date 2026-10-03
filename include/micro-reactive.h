#pragma once

/**
 * Micro-Reactive Library
 * A lightweight, C++11-compatible reactive programming library for ESP32/Arduino
 * 
 * Features:
 * - Source Observables (Empty, Never, Range, Create, Iterate, Timer, Interval, Defer, From, Just)
 * - Transform Operators (Map, Filter, Take, Skip, Debounce, CombineLatest, Merge, FlatMap, Scan, Reduce)
 * - Subjects (Subject, BehaviorSubject, ReplaySubject, SynchronizedSubject, AsyncSubject)
 * - Error Handling (Catch, Retry, Finally, OnErrorResumeNext)
 * - Schedulers (Immediate, ThreadPool, Single, Computation)
 * - Memory-safe shared_ptr based design
 * - Exception handling and error recovery
 * - Thread-safe components with proper resource management
 * - Advanced debugging and logging utilities
 */

// Core reactive interfaces and base classes
#include "core.h"

// Source observables - create reactive streams
#include "sources.h"

// Subjects - both observer and observable
#include "subjects.h"

// Transform operators - modify reactive streams (now includes advanced operators)
#include "operators.h"

// Error handling operators and utilities
#include "error_handling.h"

// Scheduler interfaces for execution control
#include "scheduler.h"
