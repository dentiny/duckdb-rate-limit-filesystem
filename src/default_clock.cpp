#include "default_clock.hpp"

#include "duckdb/common/helper.hpp"

#include <thread>

namespace duckdb {

TimePoint DefaultClock::Now() const {
	return std::chrono::steady_clock::now();
}

void DefaultClock::SleepFor(Duration duration) const {
	std::this_thread::sleep_for(duration);
}

void DefaultClock::SleepUntil(TimePoint time_point) const {
	std::this_thread::sleep_until(time_point);
}

shared_ptr<BaseClock> CreateDefaultClock() {
	return make_shared_ptr<DefaultClock>();
}

} // namespace duckdb
