#pragma once

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

// Time point type used throughout the throttle layer.
using TimePoint = std::chrono::steady_clock::time_point;

// Duration type used throughout the throttle layer.
using Duration = std::chrono::nanoseconds;

// Abstract clock interface for time keeping.
//
// Provides an abstraction over time sources, enabling:
// - Real-time execution with the default clock
// - Deterministic testing with mock clocks
// - Custom time sources for specialized use cases
class BaseClock {
public:
	virtual ~BaseClock() = default;

	// Returns the current time point.
	virtual TimePoint Now() const = 0;

	// Sleeps for the specified duration.
	virtual void SleepFor(Duration duration) const = 0;

	// Sleeps until the specified time point.
	virtual void SleepUntil(TimePoint time_point) const = 0;
};

} // namespace duckdb
