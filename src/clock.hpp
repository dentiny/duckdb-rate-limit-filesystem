#pragma once

#include <chrono>
#include <memory>
#include <thread>

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
 * @class Clock
 * @brief Abstract clock interface for time keeping.
 * @details Provides an abstraction over time sources, enabling:
 *          - Real-time execution with the default clock
 *          - Deterministic testing with mock clocks
 *          - Custom time sources for specialized use cases
 */
class Clock {
 public:
  virtual ~Clock() = default;

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

/**
 * @class DefaultClock
 * @brief Default clock implementation using std::chrono::steady_clock.
 * @details Uses the system's steady clock for real-time operations.
 *          Thread-safe and suitable for production use.
 */
class DefaultClock : public Clock {
 public:
  TimePoint Now() const override {
    return std::chrono::steady_clock::now();
  }

  void SleepFor(Duration duration) const override {
    std::this_thread::sleep_for(duration);
  }

  void SleepUntil(TimePoint time_point) const override {
    std::this_thread::sleep_until(time_point);
  }
};

/**
 * @class MockClock
 * @brief Mock clock for testing purposes.
 * @details Allows manual control of time for deterministic testing.
 *          Not thread-safe - intended for single-threaded test scenarios.
 */
class MockClock : public Clock {
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
 * @brief Create a default clock instance.
 * @return Shared pointer to a DefaultClock
 */
inline std::shared_ptr<Clock> CreateDefaultClock() {
  return std::make_shared<DefaultClock>();
}

/**
 * @brief Create a mock clock instance for testing.
 * @param initial_time Optional initial time point
 * @return Shared pointer to a MockClock
 */
inline std::shared_ptr<MockClock> CreateMockClock(
    TimePoint initial_time = TimePoint{}) {
  return std::make_shared<MockClock>(initial_time);
}

}  // namespace duckdb

