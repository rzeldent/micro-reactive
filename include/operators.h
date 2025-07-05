#ifndef MICRO_REACTIVE_OPERATORS_H
#define MICRO_REACTIVE_OPERATORS_H

#include "core.h"
#include <functional>
#include <memory>
#include <vector>
#include <cstddef>

namespace rx {

// =============================================================================
// MAP OPERATOR - Transforms each emitted item by applying a function
// =============================================================================
template <typename Tsrc, typename Tdest>
class MapOperator : public Operator<Tdest> {
    class MapObserver : public IObserver<Tsrc> {
    private:
        Operator<Tdest> *_operator;
        std::function<Tdest(const Tsrc &)> _transform;

    public:
        MapObserver(Operator<Tdest> *op, std::function<Tdest(const Tsrc &)> transform)
            : _operator(op), _transform(transform) {
        }

        void OnNext(const Tsrc &value) override {
            _operator->NotifyOnNext(_transform(value));
        }

        void OnCompleted() override {
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<Tsrc>> _observable;
    std::shared_ptr<MapObserver> _observer;

public:
    MapOperator(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform)
        : _observable(observable) {
        _observer = std::make_shared<MapObserver>(this, transform);
    }

    void Subscribe(std::shared_ptr<IObserver<Tdest>> observer) override {
        Operator<Tdest>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<Tdest>> observer) override {
        Operator<Tdest>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename Tsrc, typename Tdest>
std::shared_ptr<MapOperator<Tsrc, Tdest>> Map(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform) {
    return std::make_shared<MapOperator<Tsrc, Tdest>>(observable, transform);
}

// =============================================================================
// FILTER OPERATOR - Only emits items that pass a predicate test
// =============================================================================
template <typename T>
class FilterOperator : public Operator<T> {
    class FilterObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        std::function<bool(const T &)> _predicate;

    public:
        FilterObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
            : _operator(op), _predicate(predicate) {
        }

        void OnNext(const T &value) override {
            if (_predicate(value))
                _operator->NotifyOnNext(value);
        }

        void OnCompleted() override {
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<FilterObserver> _observer;

public:
    FilterOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
        : _observable(observable) {
        _observer = std::make_shared<FilterObserver>(this, predicate);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<FilterOperator<T>> Filter(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate) {
    return std::make_shared<FilterOperator<T>>(observable, predicate);
}

// =============================================================================
// TAKE OPERATOR - Emits only the first n items
// =============================================================================
template <typename T>
class TakeOperator : public Operator<T> {
    class TakeObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        size_t _count;
        size_t _taken = 0;

    public:
        TakeObserver(Operator<T> *op, size_t count)
            : _operator(op), _count(count) {
        }

        void OnNext(const T &value) override {
            if (_taken < _count) {
                _operator->NotifyOnNext(value);
                _taken++;
                if (_taken == _count)
                    _operator->NotifyOnCompleted();
            }
        }

        void OnCompleted() override {
            if (_taken < _count)
                _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<TakeObserver> _observer;

public:
    TakeOperator(std::shared_ptr<IObservable<T>> observable, size_t count)
        : _observable(observable) {
        _observer = std::make_shared<TakeObserver>(this, count);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<TakeOperator<T>> Take(std::shared_ptr<IObservable<T>> observable, size_t count) {
    return std::make_shared<TakeOperator<T>>(observable, count);
}

// =============================================================================
// SKIP OPERATOR - Skips the first n items
// =============================================================================
template <typename T>
class SkipOperator : public Operator<T> {
    class SkipObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        size_t _count;
        size_t _skipped = 0;

    public:
        SkipObserver(Operator<T> *op, size_t count)
            : _operator(op), _count(count) {
        }

        void OnNext(const T &value) override {
            if (_skipped < _count) {
                _skipped++;
            } else {
                _operator->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<SkipObserver> _observer;

public:
    SkipOperator(std::shared_ptr<IObservable<T>> observable, size_t count)
        : _observable(observable) {
        _observer = std::make_shared<SkipObserver>(this, count);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<SkipOperator<T>> Skip(std::shared_ptr<IObservable<T>> observable, size_t count) {
    return std::make_shared<SkipOperator<T>>(observable, count);
}

// =============================================================================
// DISTINCT OPERATOR - Emits only distinct items (removes duplicates)
// =============================================================================
template <typename T>
class DistinctOperator : public Operator<T> {
    class DistinctObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        std::vector<T> _seen;

    public:
        DistinctObserver(Operator<T> *op) : _operator(op) {}

        void OnNext(const T &value) override {
            // Check if we've seen this value before
            bool found = false;
            for (const auto& seen : _seen) {
                if (seen == value) {
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                _seen.push_back(value);
                _operator->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<DistinctObserver> _observer;

public:
    DistinctOperator(std::shared_ptr<IObservable<T>> observable)
        : _observable(observable) {
        _observer = std::make_shared<DistinctObserver>(this);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<DistinctOperator<T>> Distinct(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<DistinctOperator<T>>(observable);
}

// =============================================================================
// SCAN OPERATOR - Applies an accumulator function and emits each result
// =============================================================================
template <typename T, typename TAcc>
class ScanOperator : public Operator<TAcc> {
    class ScanObserver : public IObserver<T> {
    private:
        Operator<TAcc> *_operator;
        std::function<TAcc(const TAcc&, const T&)> _accumulator;
        TAcc _seed;
        bool _first = true;

    public:
        ScanObserver(Operator<TAcc> *op, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator)
            : _operator(op), _seed(seed), _accumulator(accumulator) {}

        void OnNext(const T &value) override {
            if (_first) {
                _seed = _accumulator(_seed, value);
                _first = false;
            } else {
                _seed = _accumulator(_seed, value);
            }
            _operator->NotifyOnNext(_seed);
        }

        void OnCompleted() override {
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<ScanObserver> _observer;

public:
    ScanOperator(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator)
        : _observable(observable) {
        _observer = std::make_shared<ScanObserver>(this, seed, accumulator);
    }

    void Subscribe(std::shared_ptr<IObserver<TAcc>> observer) override {
        Operator<TAcc>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<TAcc>> observer) override {
        Operator<TAcc>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T, typename TAcc>
std::shared_ptr<ScanOperator<T, TAcc>> Scan(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator) {
    return std::make_shared<ScanOperator<T, TAcc>>(observable, seed, accumulator);
}

// =============================================================================
// REDUCE OPERATOR - Applies an accumulator function and emits only the final result
// =============================================================================
template <typename T, typename TAcc>
class ReduceOperator : public Operator<TAcc> {
    class ReduceObserver : public IObserver<T> {
    private:
        Operator<TAcc> *_operator;
        std::function<TAcc(const TAcc&, const T&)> _accumulator;
        TAcc _seed;

    public:
        ReduceObserver(Operator<TAcc> *op, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator)
            : _operator(op), _seed(seed), _accumulator(accumulator) {}

        void OnNext(const T &value) override {
            _seed = _accumulator(_seed, value);
        }

        void OnCompleted() override {
            _operator->NotifyOnNext(_seed);
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<ReduceObserver> _observer;

public:
    ReduceOperator(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator)
        : _observable(observable) {
        _observer = std::make_shared<ReduceObserver>(this, seed, accumulator);
    }

    void Subscribe(std::shared_ptr<IObserver<TAcc>> observer) override {
        Operator<TAcc>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<TAcc>> observer) override {
        Operator<TAcc>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T, typename TAcc>
std::shared_ptr<ReduceOperator<T, TAcc>> Reduce(std::shared_ptr<IObservable<T>> observable, TAcc seed, std::function<TAcc(const TAcc&, const T&)> accumulator) {
    return std::make_shared<ReduceOperator<T, TAcc>>(observable, seed, accumulator);
}

// =============================================================================
// THROTTLE OPERATOR - Emits an item only if a particular timespan has passed without emitting another item
// Note: Simplified version for embedded systems without complex timing
// =============================================================================
template <typename T>
class ThrottleOperator : public Operator<T> {
    class ThrottleObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        size_t _interval;
        size_t _count = 0;

    public:
        ThrottleObserver(Operator<T> *op, size_t interval)
            : _operator(op), _interval(interval) {}

        void OnNext(const T &value) override {
            _count++;
            if (_count % _interval == 0) {
                _operator->NotifyOnNext(value);
            }
        }

        void OnCompleted() override {
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<ThrottleObserver> _observer;

public:
    ThrottleOperator(std::shared_ptr<IObservable<T>> observable, size_t interval)
        : _observable(observable) {
        _observer = std::make_shared<ThrottleObserver>(this, interval);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<ThrottleOperator<T>> Throttle(std::shared_ptr<IObservable<T>> observable, size_t interval) {
    return std::make_shared<ThrottleOperator<T>>(observable, interval);
}

// =============================================================================
// FIRST OPERATOR - Emits only the first item
// =============================================================================
template <typename T>
class FirstOperator : public Operator<T> {
    class FirstObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        bool _emitted = false;

    public:
        FirstObserver(Operator<T> *op) : _operator(op) {}

        void OnNext(const T &value) override {
            if (!_emitted) {
                _emitted = true;
                _operator->NotifyOnNext(value);
                _operator->NotifyOnCompleted();
            }
        }

        void OnCompleted() override {
            if (!_emitted) {
                _operator->NotifyOnCompleted();
            }
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<FirstObserver> _observer;

public:
    FirstOperator(std::shared_ptr<IObservable<T>> observable)
        : _observable(observable) {
        _observer = std::make_shared<FirstObserver>(this);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<FirstOperator<T>> First(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<FirstOperator<T>>(observable);
}

// =============================================================================
// LAST OPERATOR - Emits only the last item
// =============================================================================
template <typename T>
class LastOperator : public Operator<T> {
    class LastObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        T _lastValue;
        bool _hasValue = false;

    public:
        LastObserver(Operator<T> *op) : _operator(op) {}

        void OnNext(const T &value) override {
            _lastValue = value;
            _hasValue = true;
        }

        void OnCompleted() override {
            if (_hasValue) {
                _operator->NotifyOnNext(_lastValue);
            }
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<LastObserver> _observer;

public:
    LastOperator(std::shared_ptr<IObservable<T>> observable)
        : _observable(observable) {
        _observer = std::make_shared<LastObserver>(this);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<LastOperator<T>> Last(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<LastOperator<T>>(observable);
}

// =============================================================================
// WHERE OPERATOR - Alias for Filter (common in LINQ)
// =============================================================================
template <typename T>
std::shared_ptr<FilterOperator<T>> Where(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate) {
    return Filter(observable, predicate);
}

// =============================================================================
// SELECT OPERATOR - Alias for Map (common in LINQ)
// =============================================================================
template <typename Tsrc, typename Tdest>
std::shared_ptr<MapOperator<Tsrc, Tdest>> Select(std::shared_ptr<IObservable<Tsrc>> observable, std::function<Tdest(const Tsrc &)> transform) {
    return Map<Tsrc, Tdest>(observable, transform);
}

// =============================================================================
// BUFFER OPERATOR - Groups emitted items into buffers of a specified size
// =============================================================================
template <typename T>
class BufferOperator : public Operator<std::vector<T>> {
    class BufferObserver : public IObserver<T> {
    private:
        Operator<std::vector<T>> *_operator;
        size_t _bufferSize;
        std::vector<T> _buffer;

    public:
        BufferObserver(Operator<std::vector<T>> *op, size_t bufferSize)
            : _operator(op), _bufferSize(bufferSize) {
        }

        void OnNext(const T &value) override {
            _buffer.push_back(value);
            if (_buffer.size() >= _bufferSize) {
                _operator->NotifyOnNext(_buffer);
                _buffer.clear();
            }
        }

        void OnCompleted() override {
            if (!_buffer.empty()) {
                _operator->NotifyOnNext(_buffer);
            }
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<BufferObserver> _observer;

public:
    BufferOperator(std::shared_ptr<IObservable<T>> observable, size_t bufferSize)
        : _observable(observable) {
        _observer = std::make_shared<BufferObserver>(this, bufferSize);
    }

    void Subscribe(std::shared_ptr<IObserver<std::vector<T>>> observer) override {
        Operator<std::vector<T>>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<std::vector<T>>> observer) override {
        Operator<std::vector<T>>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<BufferOperator<T>> Buffer(std::shared_ptr<IObservable<T>> observable, size_t bufferSize) {
    return std::make_shared<BufferOperator<T>>(observable, bufferSize);
}

// =============================================================================
// TAKEWHILE OPERATOR - Takes items while a condition is true
// =============================================================================
template <typename T>
class TakeWhileOperator : public Operator<T> {
    class TakeWhileObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        std::function<bool(const T &)> _predicate;
        bool _completed;

    public:
        TakeWhileObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
            : _operator(op), _predicate(predicate), _completed(false) {
        }

        void OnNext(const T &value) override {
            if (!_completed && _predicate(value)) {
                _operator->NotifyOnNext(value);
            } else if (!_completed) {
                _completed = true;
                _operator->NotifyOnCompleted();
            }
        }

        void OnCompleted() override {
            if (!_completed) {
                _operator->NotifyOnCompleted();
            }
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<TakeWhileObserver> _observer;

public:
    TakeWhileOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
        : _observable(observable) {
        _observer = std::make_shared<TakeWhileObserver>(this, predicate);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<TakeWhileOperator<T>> TakeWhile(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate) {
    return std::make_shared<TakeWhileOperator<T>>(observable, predicate);
}

// =============================================================================
// SKIPWHILE OPERATOR - Skips items while a condition is true
// =============================================================================
template <typename T>
class SkipWhileOperator : public Operator<T> {
    class SkipWhileObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        std::function<bool(const T &)> _predicate;
        bool _skipping;

    public:
        SkipWhileObserver(Operator<T> *op, std::function<bool(const T &)> predicate)
            : _operator(op), _predicate(predicate), _skipping(true) {
        }

        void OnNext(const T &value) override {
            if (_skipping && _predicate(value)) {
                return; // Skip this value
            }
            _skipping = false; // Stop skipping once condition fails
            _operator->NotifyOnNext(value);
        }

        void OnCompleted() override {
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<SkipWhileObserver> _observer;

public:
    SkipWhileOperator(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate)
        : _observable(observable) {
        _observer = std::make_shared<SkipWhileObserver>(this, predicate);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<SkipWhileOperator<T>> SkipWhile(std::shared_ptr<IObservable<T>> observable, std::function<bool(const T &)> predicate) {
    return std::make_shared<SkipWhileOperator<T>>(observable, predicate);
}

// =============================================================================
// STARTWITH OPERATOR - Prepends values to the beginning of the sequence
// =============================================================================
template <typename T>
class StartWithOperator : public Operator<T> {
    class StartWithObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;

    public:
        StartWithObserver(Operator<T> *op) : _operator(op) {
        }

        void OnNext(const T &value) override {
            _operator->NotifyOnNext(value);
        }

        void OnCompleted() override {
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<StartWithObserver> _observer;
    std::vector<T> _startValues;

public:
    StartWithOperator(std::shared_ptr<IObservable<T>> observable, std::vector<T> startValues)
        : _observable(observable), _startValues(startValues) {
        _observer = std::make_shared<StartWithObserver>(this);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1) {
            // Emit start values first
            for (const auto& value : _startValues) {
                this->NotifyOnNext(value);
            }
            _observable->Subscribe(_observer);
        }
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<StartWithOperator<T>> StartWith(std::shared_ptr<IObservable<T>> observable, std::vector<T> startValues) {
    return std::make_shared<StartWithOperator<T>>(observable, startValues);
}

template <typename T>
std::shared_ptr<StartWithOperator<T>> StartWith(std::shared_ptr<IObservable<T>> observable, T startValue) {
    return std::make_shared<StartWithOperator<T>>(observable, std::vector<T>{startValue});
}

// =============================================================================
// DEFAULTIFEMPTY OPERATOR - Emits a default value if the sequence is empty
// =============================================================================
template <typename T>
class DefaultIfEmptyOperator : public Operator<T> {
    class DefaultIfEmptyObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        T _defaultValue;
        bool _hasEmitted;

    public:
        DefaultIfEmptyObserver(Operator<T> *op, T defaultValue)
            : _operator(op), _defaultValue(defaultValue), _hasEmitted(false) {
        }

        void OnNext(const T &value) override {
            _hasEmitted = true;
            _operator->NotifyOnNext(value);
        }

        void OnCompleted() override {
            if (!_hasEmitted) {
                _operator->NotifyOnNext(_defaultValue);
            }
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<DefaultIfEmptyObserver> _observer;

public:
    DefaultIfEmptyOperator(std::shared_ptr<IObservable<T>> observable, T defaultValue)
        : _observable(observable) {
        _observer = std::make_shared<DefaultIfEmptyObserver>(this, defaultValue);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<DefaultIfEmptyOperator<T>> DefaultIfEmpty(std::shared_ptr<IObservable<T>> observable, T defaultValue) {
    return std::make_shared<DefaultIfEmptyOperator<T>>(observable, defaultValue);
}

// =============================================================================
// COUNT OPERATOR - Counts the number of items emitted
// =============================================================================
template <typename T>
class CountOperator : public Operator<size_t> {
    class CountObserver : public IObserver<T> {
    private:
        Operator<size_t> *_operator;
        size_t _count;

    public:
        CountObserver(Operator<size_t> *op) : _operator(op), _count(0) {
        }

        void OnNext(const T &value) override {
            _count++;
        }

        void OnCompleted() override {
            _operator->NotifyOnNext(_count);
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<CountObserver> _observer;

public:
    CountOperator(std::shared_ptr<IObservable<T>> observable)
        : _observable(observable) {
        _observer = std::make_shared<CountObserver>(this);
    }

    void Subscribe(std::shared_ptr<IObserver<size_t>> observer) override {
        Operator<size_t>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<size_t>> observer) override {
        Operator<size_t>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<CountOperator<T>> Count(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<CountOperator<T>>(observable);
}

// =============================================================================
// SUM OPERATOR - Calculates the sum of numeric items
// =============================================================================
template <typename T>
class SumOperator : public Operator<T> {
    class SumObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        T _sum;

    public:
        SumObserver(Operator<T> *op) : _operator(op), _sum(T{}) {
        }

        void OnNext(const T &value) override {
            _sum += value;
        }

        void OnCompleted() override {
            _operator->NotifyOnNext(_sum);
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<SumObserver> _observer;

public:
    SumOperator(std::shared_ptr<IObservable<T>> observable)
        : _observable(observable) {
        _observer = std::make_shared<SumObserver>(this);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<SumOperator<T>> Sum(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<SumOperator<T>>(observable);
}

// =============================================================================
// MIN OPERATOR - Finds the minimum value
// =============================================================================
template <typename T>
class MinOperator : public Operator<T> {
    class MinObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        T _min;
        bool _hasValue;

    public:
        MinObserver(Operator<T> *op) : _operator(op), _min(T{}), _hasValue(false) {
        }

        void OnNext(const T &value) override {
            if (!_hasValue || value < _min) {
                _min = value;
                _hasValue = true;
            }
        }

        void OnCompleted() override {
            if (_hasValue) {
                _operator->NotifyOnNext(_min);
            }
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<MinObserver> _observer;

public:
    MinOperator(std::shared_ptr<IObservable<T>> observable)
        : _observable(observable) {
        _observer = std::make_shared<MinObserver>(this);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<MinOperator<T>> Min(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<MinOperator<T>>(observable);
}

// =============================================================================
// MAX OPERATOR - Finds the maximum value
// =============================================================================
template <typename T>
class MaxOperator : public Operator<T> {
    class MaxObserver : public IObserver<T> {
    private:
        Operator<T> *_operator;
        T _max;
        bool _hasValue;

    public:
        MaxObserver(Operator<T> *op) : _operator(op), _max(T{}), _hasValue(false) {
        }

        void OnNext(const T &value) override {
            if (!_hasValue || value > _max) {
                _max = value;
                _hasValue = true;
            }
        }

        void OnCompleted() override {
            if (_hasValue) {
                _operator->NotifyOnNext(_max);
            }
            _operator->NotifyOnCompleted();
        }

        void OnError(const std::exception &e) override {
            _operator->NotifyOnError(e);
        }
    };

    std::shared_ptr<IObservable<T>> _observable;
    std::shared_ptr<MaxObserver> _observer;

public:
    MaxOperator(std::shared_ptr<IObservable<T>> observable)
        : _observable(observable) {
        _observer = std::make_shared<MaxObserver>(this);
    }

    void Subscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::Subscribe(observer);
        if (this->_childObservers.size() == 1)
            _observable->Subscribe(_observer);
    }

    void UnSubscribe(std::shared_ptr<IObserver<T>> observer) override {
        Operator<T>::UnSubscribe(observer);
        if (this->_childObservers.empty())
            _observable->UnSubscribe(_observer);
    }
};

template <typename T>
std::shared_ptr<MaxOperator<T>> Max(std::shared_ptr<IObservable<T>> observable) {
    return std::make_shared<MaxOperator<T>>(observable);
}

} // namespace rx

#endif // MICRO_REACTIVE_OPERATORS_H
