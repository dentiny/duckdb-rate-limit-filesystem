#pragma once

#include "base_clock.hpp"

#include "duckdb/common/helper.hpp"

namespace duckdb {

/**
 * @class MockClock
 * @brief Mock clock for testing purposes.
 * @details Allows manual control of time for deterministic testing.
 *          Not thread-safe - intended for single-threaded test scenarios.
 */
class MockClock : public BaseClock {
 public:
  explicit MockClock(TimePoint initial_time = TimePoint{})
      : current_time(initial_time) {}

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

  /**
   * @brief Advance the mock clock by the specified duration.
   * @param duration_p The duration to advance by
   */
  void Advance(Duration duration_p) {
    current_time += duration_p;
  }

  /**
   * @brief Set the mock clock to a specific time point.
   * @param time_point The time point to set
   */
  void SetTime(TimePoint time_point) {
    current_time = time_point;
  }

 private:
  mutable TimePoint current_time;
};

/**
 * @brief Create a mock clock instance for testing.
 * @param initial_time Optional initial time point
 * @return Shared pointer to a MockClock
 */
inline shared_ptr<MockClock> CreateMockClock(
    TimePoint initial_time = TimePoint{}) {
  return make_shared_ptr<MockClock>(initial_time);
}

}  // namespace duckdb
