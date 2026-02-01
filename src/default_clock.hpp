#pragma once

#include "base_clock.hpp"

#include <thread>

namespace duckdb {

/**
 * @class DefaultClock
 * @brief Default clock implementation using std::chrono::steady_clock.
 * @details Uses the system's steady clock for real-time operations.
 *          Thread-safe and suitable for production use.
 */
class DefaultClock : public BaseClock {
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
 * @brief Create a default clock instance.
 * @return Shared pointer to a DefaultClock
 */
inline std::shared_ptr<BaseClock> CreateDefaultClock() {
  return std::make_shared<DefaultClock>();
}

}  // namespace duckdb

