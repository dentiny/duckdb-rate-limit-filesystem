#pragma once

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

/**
 * @brief Time point type used throughout the throttle layer.
 */
using TimePoint = std::chrono::steady_clock::time_point;

/**
 * @brief Duration type used throughout the throttle layer.
 */
using Duration = std::chrono::nanoseconds;

/**
 * @class BaseClock
 * @brief Abstract clock interface for time keeping.
 * @details Provides an abstraction over time sources, enabling:
 *          - Real-time execution with the default clock
 *          - Deterministic testing with mock clocks
 *          - Custom time sources for specialized use cases
 */
class BaseClock {
public:
	virtual ~BaseClock() = default;

	/**
	 * @brief Get the current time point.
	 * @return Current time as a TimePoint
	 */
	virtual TimePoint Now() const = 0;

	/**
	 * @brief Sleep for the specified duration.
	 * @param duration The duration to sleep for
	 */
	virtual void SleepFor(Duration duration) const = 0;

	/**
	 * @brief Sleep until the specified time point.
	 * @param time_point The time point to sleep until
	 */
	virtual void SleepUntil(TimePoint time_point) const = 0;
};

} // namespace duckdb
