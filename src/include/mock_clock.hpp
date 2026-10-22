#pragma once

#include "base_clock.hpp"

namespace duckdb {

// Mock clock for testing purposes.
// Allows manual control of time for deterministic testing.
// Not thread-safe - intended for single-threaded test scenarios.
class MockClock : public BaseClock {
public:
	explicit MockClock(RateLimitTimePoint initial_time = RateLimitTimePoint {});

	RateLimitTimePoint Now() const override;
	void SleepFor(Duration duration_p) const override;
	void SleepUntil(RateLimitTimePoint time_point) const override;

	// Advances the mock clock by the specified duration.
	void Advance(Duration duration_p);

	// Sets the mock clock to a specific time point.
	void SetTime(RateLimitTimePoint time_point);

private:
	mutable RateLimitTimePoint current_time;
};

// Creates a mock clock instance for testing with an optional initial time point.
shared_ptr<MockClock> CreateMockClock(RateLimitTimePoint initial_time = RateLimitTimePoint {});

} // namespace duckdb
