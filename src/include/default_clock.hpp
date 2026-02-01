#pragma once

#include "base_clock.hpp"

namespace duckdb {

// Default clock implementation with monotonic clock.
// Thread-safe and suitable for production use.
class DefaultClock : public BaseClock {
public:
	TimePoint Now() const override;
	void SleepFor(Duration duration) const override;
	void SleepUntil(TimePoint time_point) const override;
};

// Creates a default clock instance.
shared_ptr<BaseClock> CreateDefaultClock();

} // namespace duckdb
