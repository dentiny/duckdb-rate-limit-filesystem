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
      : current_time_(initial_time) {}

  TimePoint Now() const override {
    return current_time_;
  }

  void SleepFor(Duration duration) const override {
    current_time_ += duration;
  }

  void SleepUntil(TimePoint time_point) const override {
    if (time_point > current_time_) {
      current_time_ = time_point;
    }
  }

  /**
   * @brief Advance the mock clock by the specified duration.
   * @param duration The duration to advance by
   */
  void Advance(Duration duration) {
    current_time_ += duration;
  }

  /**
   * @brief Set the mock clock to a specific time point.
   * @param time_point The time point to set
   */
  void SetTime(TimePoint time_point) {
    current_time_ = time_point;
  }

 private:
  mutable TimePoint current_time_;
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
