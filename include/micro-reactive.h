#ifndef MICRO_REACTIVE_H
#define MICRO_REACTIVE_H

/**
 * Micro-Reactive Library
 * A lightweight, C++11-compatible reactive programming library for ESP32/Arduino
 * 
 * Features:
 * - Source Observables (Empty, Never, Range, Create, Iterate, Timer, Interval, Defer)
 * - Transform Operators (Map, Filter, Take, Skip)
 * - Subjects (Subject, BehaviorSubject, ReplaySubject, SynchronizedSubject)
 * - Memory-safe shared_ptr based design
 * - Exception handling support
 * - Thread-safe components
 */

// Core reactive interfaces and base classes
#include "core.h"

// Source observables - create reactive streams
#include "sources.h"

// Transform operators - modify reactive streams
#include "operators.h"

// Subjects - both observer and observable
#include "subjects.h"

#endif // MICRO_REACTIVE_H
