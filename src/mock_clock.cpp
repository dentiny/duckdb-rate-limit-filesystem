#include "mock_clock.hpp"

#include "duckdb/common/helper.hpp"

namespace duckdb {

MockClock::MockClock(TimePoint initial_time) : current_time(initial_time) {
}

TimePoint MockClock::Now() const {
	return current_time;
}

void MockClock::SleepFor(Duration duration_p) const {
	current_time += duration_p;
}

void MockClock::SleepUntil(TimePoint time_point) const {
	if (time_point > current_time) {
		current_time = time_point;
	}
}

void MockClock::Advance(Duration duration_p) {
	current_time += duration_p;
}

void MockClock::SetTime(TimePoint time_point) {
	current_time = time_point;
}

shared_ptr<MockClock> CreateMockClock(TimePoint initial_time) {
	return make_shared_ptr<MockClock>(initial_time);
}

} // namespace duckdb
