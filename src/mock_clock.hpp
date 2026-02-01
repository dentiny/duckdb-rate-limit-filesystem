#pragma once

#include "base_clock.hpp"

#include "duckdb/common/helper.hpp"

namespace duckdb {

// Mock clock for testing purposes.
// Allows manual control of time for deterministic testing.
// Not thread-safe - intended for single-threaded test scenarios.
class MockClock : public BaseClock {
public:
	explicit MockClock(TimePoint initial_time = TimePoint {}) : current_time(initial_time) {
	}

	TimePoint Now() const override {
		return current_time;
	}

	void SleepFor(Duration duration_p) const override {
		current_time += duration_p;
	}

	void SleepUntil(TimePoint time_point) const override {
		if (time_point > current_time) {
			current_time = time_point;
		}
	}

	// Advances the mock clock by the specified duration.
	void Advance(Duration duration_p) {
		current_time += duration_p;
	}

	// Sets the mock clock to a specific time point.
	void SetTime(TimePoint time_point) {
		current_time = time_point;
	}

private:
	mutable TimePoint current_time;
};

// Creates a mock clock instance for testing with an optional initial time point.
inline shared_ptr<MockClock> CreateMockClock(TimePoint initial_time = TimePoint {}) {
	return make_shared_ptr<MockClock>(initial_time);
}

} // namespace duckdb
