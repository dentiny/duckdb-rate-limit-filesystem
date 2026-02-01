#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

#include <cstdint>
#include <stdexcept>

#include "base_clock.hpp"
#include "default_clock.hpp"
#include "rate_limiter.hpp"

namespace duckdb {

/**
 * @brief Error codes for throttle layer operations.
 */
enum class ThrottleError {
  /// No error
  None,
  /// Request size exceeds the configured burst capacity
  RequestExceedsBurst,
  /// Rate limited - waiting for quota
  RateLimited,
};

/**
 * @brief Exception thrown when throttle operations fail.
 */
class ThrottleException : public std::runtime_error {
 public:
  explicit ThrottleException(ThrottleError error, const string& message)
      : std::runtime_error(message), error_(error) {}

  ThrottleError GetError() const { return error_; }

 private:
  ThrottleError error_;
};

/**
 * @brief Result type for read operations.
 */
struct ReadResult {
  /// Whether the operation succeeded
  bool success;
  /// Error code if operation failed
  ThrottleError error;
  /// Number of bytes actually read
  size_t bytes_read;
  /// Error message if operation failed
  string error_message;

  static ReadResult Success(size_t bytes) {
    return ReadResult{true, ThrottleError::None, bytes, ""};
  }

  static ReadResult Error(ThrottleError err, const string& msg) {
    return ReadResult{false, err, 0, msg};
  }
};

/**
 * @brief Result type for write operations.
 */
struct WriteResult {
  /// Whether the operation succeeded
  bool success;
  /// Error code if operation failed
  ThrottleError error;
  /// Number of bytes actually written
  size_t bytes_written;
  /// Error message if operation failed
  string error_message;

  static WriteResult Success(size_t bytes) {
    return WriteResult{true, ThrottleError::None, bytes, ""};
  }

  static WriteResult Error(ThrottleError err, const string& msg) {
    return WriteResult{false, err, 0, msg};
  }
};

/**
 * @class ThrottleLayer
 * @brief Bandwidth and API call rate limiter for I/O operations.
 *
 * @details Implements a throttle layer using the Generic Cell Rate
 *          Algorithm (GCRA). This provides smooth rate limiting with burst
 *          support for bandwidth, and simple rate limiting for API calls.
 *
 * Key features:
 * - **Bandwidth**: Maximum bytes per second allowed through the throttle
 * - **Burst**: Maximum bytes allowed in a single burst (must be >= largest
 *   single operation)
 * - **API Rate**: Maximum API calls per second (optional)
 * - **Thread-safe**: Safe for concurrent use from multiple threads
 * - **Blocking**: Operations will block until quota is available
 *
 * @note When setting the ThrottleLayer, always consider the largest possible
 *       operation size as the burst size, as the burst size should be larger
 *       than any possible byte length to allow it to pass through.
 *
 * @par Example Usage:
 * @code{.cpp}
 * // Create a throttle layer with:
 * // - 10 KiB/s bandwidth, 10 MiB burst
 * // - 100 API calls/s
 * auto throttle = ThrottleLayer(10 * 1024, 10000 * 1024, 100);
 *
 * // Read with throttling (checks both bandwidth and API rate)
 * auto result = throttle.Read("/path/to/file", 0, 4096);
 * if (result.success) {
 *     // Read succeeded, result.bytes_read contains bytes read
 * }
 *
 * // Write with throttling
 * auto write_result = throttle.Write("/path/to/file", 8192);
 * if (write_result.success) {
 *     // Write succeeded
 * }
 * @endcode
 */
class ThrottleLayer {
 public:
  /**
   * @brief Construct a new ThrottleLayer with bandwidth limiting only.
   *
   * @param bandwidth Maximum bytes per second (must be > 0)
   * @param burst Maximum bytes allowed at once (must be > 0)
   * @param clock Optional custom clock implementation (for testing)
   *
   * @throws InvalidInputException if bandwidth or burst is 0
   *
   * @par Example:
   * @code{.cpp}
   * // 10 KiB/s bandwidth, 10 MiB burst (no API rate limiting)
   * ThrottleLayer throttle(10 * 1024, 10000 * 1024);
   * @endcode
   */
  ThrottleLayer(uint32_t bandwidth, uint32_t burst,
                shared_ptr<BaseClock> clock = nullptr);

  /**
   * @brief Construct a new ThrottleLayer with both bandwidth and API rate
   *        limiting.
   *
   * @param bandwidth Maximum bytes per second (must be > 0)
   * @param burst Maximum bytes allowed at once (must be > 0)
   * @param api_rate Maximum API calls per second (must be > 0)
   * @param clock Optional custom clock implementation (for testing)
   *
   * @throws InvalidInputException if any parameter is 0
   *
   * @par Example:
   * @code{.cpp}
   * // 10 KiB/s bandwidth, 10 MiB burst, 100 API calls/s
   * ThrottleLayer throttle(10 * 1024, 10000 * 1024, 100);
   * @endcode
   */
  ThrottleLayer(uint32_t bandwidth, uint32_t burst, uint32_t api_rate,
                shared_ptr<BaseClock> clock = nullptr);

  /**
   * @brief Copy constructor.
   * @details Creates a new ThrottleLayer sharing the same rate limiter
   *          state. This means multiple copies will share the same
   *          bandwidth quota.
   */
  ThrottleLayer(const ThrottleLayer& other);

  /**
   * @brief Copy assignment operator.
   */
  ThrottleLayer& operator=(const ThrottleLayer& other);

  /**
   * @brief Move constructor.
   */
  ThrottleLayer(ThrottleLayer&& other) noexcept;

  /**
   * @brief Move assignment operator.
   */
  ThrottleLayer& operator=(ThrottleLayer&& other) noexcept;

  /**
   * @brief Destructor.
   */
  ~ThrottleLayer();

  /**
   * @brief Read data with bandwidth and API rate throttling.
   *
   * @details This method simulates a throttled read operation. It blocks
   *          until sufficient bandwidth and API quota is available, then
   *          returns immediately (the actual read would be performed by
   *          the inner operator in a real implementation).
   *
   * @param path Path to the file to read
   * @param start_offset Starting offset in the file (bytes)
   * @param bytes_to_read Number of bytes to read
   *
   * @return ReadResult containing success status and bytes read
   *
   * @note If bytes_to_read exceeds the burst size, the operation will fail
   *       with ThrottleError::RequestExceedsBurst.
   *
   * @par Example:
   * @code{.cpp}
   * auto result = throttle.Read("/data/file.bin", 0, 4096);
   * if (!result.success) {
   *     std::cerr << "Read failed: " << result.error_message << std::endl;
   * }
   * @endcode
   */
  ReadResult Read(const string& path, int start_offset, int bytes_to_read);

  /**
   * @brief Write data with bandwidth and API rate throttling.
   *
   * @details This method applies bandwidth and API rate throttling before
   *          allowing a write operation. It blocks until sufficient quota
   *          is available.
   *
   * @param path Path to the file to write
   * @param bytes_to_write Number of bytes to write
   *
   * @return WriteResult containing success status and bytes written
   *
   * @note If bytes_to_write exceeds the burst size, the operation will fail
   *       with ThrottleError::RequestExceedsBurst.
   *
   * @par Example:
   * @code{.cpp}
   * auto result = throttle.Write("/data/output.bin", 8192);
   * if (!result.success) {
   *     std::cerr << "Write failed: " << result.error_message << std::endl;
   * }
   * @endcode
   */
  WriteResult Write(const string& path, int bytes_to_write);

  /**
   * @brief Get the configured bandwidth.
   * @return Bandwidth in bytes per second
   */
  uint32_t GetBandwidth() const;

  /**
   * @brief Get the configured burst size.
   * @return Burst size in bytes
   */
  uint32_t GetBurst() const;

  /**
   * @brief Get the configured API rate limit.
   * @return API calls per second, or 0 if not configured
   */
  uint32_t GetApiRate() const;

  /**
   * @brief Check if API rate limiting is enabled.
   * @return true if API rate limiting is configured
   */
  bool HasApiRateLimiting() const;

  /**
   * @brief Get the bandwidth rate limiter (for advanced use cases).
   * @return Shared pointer to the bandwidth rate limiter
   */
  SharedRateLimiter GetBandwidthRateLimiter() const;

  /**
   * @brief Get the API rate limiter (for advanced use cases).
   * @return Shared pointer to the API rate limiter, or nullptr if not
   *         configured
   */
  SharedRateLimiter GetApiRateLimiter() const;

 private:
  /// Configured bandwidth (bytes per second)
  uint32_t bandwidth_;
  /// Configured burst size (bytes)
  uint32_t burst_;
  /// Configured API rate (calls per second), 0 if disabled
  uint32_t api_rate_;
  /// Shared rate limiter for bandwidth
  SharedRateLimiter bandwidth_limiter_;
  /// Shared rate limiter for API calls (nullptr if disabled)
  SharedRateLimiter api_limiter_;
};

/**
 * @brief Builder class for creating ThrottleLayer instances.
 *
 * @details Provides a fluent interface for configuring ThrottleLayer.
 *
 * @par Example:
 * @code{.cpp}
 * auto throttle = ThrottleLayerBuilder()
 *     .WithBandwidth(10 * 1024)     // 10 KiB/s
 *     .WithBurst(10000 * 1024)      // 10 MiB
 *     .WithApiRate(100)             // 100 calls/s
 *     .Build();
 * @endcode
 */
class ThrottleLayerBuilder {
 public:
  /**
   * @brief Set the bandwidth.
   * @param bandwidth Bytes per second
   * @return Reference to this builder
   */
  ThrottleLayerBuilder& WithBandwidth(uint32_t bandwidth) {
    bandwidth_ = bandwidth;
    return *this;
  }

  /**
   * @brief Set the burst size.
   * @param burst Maximum bytes at once
   * @return Reference to this builder
   */
  ThrottleLayerBuilder& WithBurst(uint32_t burst) {
    burst_ = burst;
    return *this;
  }

  /**
   * @brief Set the API rate limit.
   * @param api_rate Maximum API calls per second
   * @return Reference to this builder
   */
  ThrottleLayerBuilder& WithApiRate(uint32_t api_rate) {
    api_rate_ = api_rate;
    return *this;
  }

  /**
   * @brief Set a custom clock (for testing).
   * @param clock Clock implementation
   * @return Reference to this builder
   */
  ThrottleLayerBuilder& WithClock(shared_ptr<BaseClock> clock) {
    clock_ = clock;
    return *this;
  }

  /**
   * @brief Build the ThrottleLayer.
   * @return Configured ThrottleLayer instance
   * @throws InvalidInputException if bandwidth or burst is not set or is 0
   */
  ThrottleLayer Build() const {
    if (bandwidth_ == 0) {
      throw InvalidInputException("bandwidth must be set and > 0");
    }
    if (burst_ == 0) {
      throw InvalidInputException("burst must be set and > 0");
    }
    if (api_rate_ > 0) {
      return ThrottleLayer(bandwidth_, burst_, api_rate_, clock_);
    }
    return ThrottleLayer(bandwidth_, burst_, clock_);
  }

 private:
  uint32_t bandwidth_ = 0;
  uint32_t burst_ = 0;
  uint32_t api_rate_ = 0;
  shared_ptr<BaseClock> clock_ = nullptr;
};

}  // namespace duckdb
