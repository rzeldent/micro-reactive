#ifndef MICRO_REACTIVE_OPERATORS_H
#define MICRO_REACTIVE_OPERATORS_H

// Include all operator groups as per copilot instructions
// This keeps all the operator factories together while maintaining modularity

// Basic utility operators
#include "operators/basic.h"

// Transformation operators (Map, Scan)
#include "operators/transformation.h"

// Filtering operators (Filter, Take, Skip, TakeWhile, SkipWhile, First, Last, Distinct)
#include "operators/filtering.h"

// Utility operators (Do, Contains, Throttle, TakeUntil, SkipUntil, DistinctUntilChanged, Pairwise)
#include "operators/utility.h"

// Aggregation operators (Reduce, All, Any, Count, Sum, Average, Min, Max)
#include "operators/aggregation.h"

// Combination operators (Race)
#include "operators/combination.h"

#endif // MICRO_REACTIVE_OPERATORS_H
