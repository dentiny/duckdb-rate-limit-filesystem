#pragma once

#include "base_clock.hpp"

namespace duckdb {

// Mock clock for testing purposes.
// Allows manual control of time for deterministic testing.
// Not thread-safe - intended for single-threaded test scenarios.
class MockClock : public BaseClock {
public:
	explicit MockClock(TimePoint initial_time = TimePoint {});

	TimePoint Now() const override;
	void SleepFor(Duration duration_p) const override;
	void SleepUntil(TimePoint time_point) const override;

	// Advances the mock clock by the specified duration.
	void Advance(Duration duration_p);

	// Sets the mock clock to a specific time point.
	void SetTime(TimePoint time_point);

private:
	mutable TimePoint current_time;
};

// Creates a mock clock instance for testing with an optional initial time point.
shared_ptr<MockClock> CreateMockClock(TimePoint initial_time = TimePoint {});

} // namespace duckdb
