#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"

#include <stdexcept>

#include "base_clock.hpp"
#include "default_clock.hpp"
#include "rate_limiter.hpp"

namespace duckdb {

// Error codes for throttle layer operations.
enum class ThrottleError {
	// No error
	None,
	// Request size exceeds the configured burst capacity
	RequestExceedsBurst,
	// Rate limited - waiting for quota
	RateLimited,
};

// Exception thrown when throttle operations fail.
class ThrottleException : public std::runtime_error {
public:
	explicit ThrottleException(ThrottleError error_p, const string &message)
	    : std::runtime_error(message), error(error_p) {
	}

	ThrottleError GetError() const {
		return error;
	}

private:
	ThrottleError error;
};

// Result type for read operations.
struct ReadResult {
	// Whether the operation succeeded
	bool success;
	// Error code if operation failed
	ThrottleError error;
	// Number of bytes actually read
	idx_t bytes_read;
	// Error message if operation failed
	string error_message;

	static ReadResult Success(idx_t bytes) {
		return ReadResult {true, ThrottleError::None, bytes, ""};
	}

	static ReadResult Error(ThrottleError err, const string &msg) {
		return ReadResult {false, err, 0, msg};
	}
};

// Result type for write operations.
struct WriteResult {
	// Whether the operation succeeded
	bool success;
	// Error code if operation failed
	ThrottleError error;
	// Number of bytes actually written
	idx_t bytes_written;
	// Error message if operation failed
	string error_message;

	static WriteResult Success(idx_t bytes) {
		return WriteResult {true, ThrottleError::None, bytes, ""};
	}

	static WriteResult Error(ThrottleError err, const string &msg) {
		return WriteResult {false, err, 0, msg};
	}
};

// Bandwidth and API call rate limiter for I/O operations.
//
// Implements a throttle layer using the Generic Cell Rate Algorithm (GCRA).
// This provides smooth rate limiting with burst support for bandwidth, and simple
// rate limiting for API calls.
//
// Key features:
// - Bandwidth: Maximum bytes per second allowed through the throttle
// - Burst: Maximum bytes allowed in a single burst (must be >= largest single operation)
// - API Rate: Maximum API calls per second (optional)
// - Thread-safe: Safe for concurrent use from multiple threads
// - Blocking: Operations will block until quota is available
//
// When setting the ThrottleLayer, always consider the largest possible operation size
// as the burst size, as the burst size should be larger than any possible byte length
// to allow it to pass through.
//
// Example usage:
//   // Create a throttle layer with:
//   // - 10 KiB/s bandwidth, 10 MiB burst
//   // - 100 API calls/s
//   auto throttle = ThrottleLayer(10 * 1024, 10000 * 1024, 100);
//
//   // Read with throttling (checks both bandwidth and API rate)
//   auto result = throttle.Read("/path/to/file", 0, 4096);
//   if (result.success) {
//       // Read succeeded, result.bytes_read contains bytes read
//   }
//
//   // Write with throttling
//   auto write_result = throttle.Write("/path/to/file", 8192);
//   if (write_result.success) {
//       // Write succeeded
//   }
class ThrottleLayer {
public:
	// Constructs a ThrottleLayer with bandwidth limiting only.
	// Both bandwidth and burst must be > 0, otherwise throws InvalidInputException.
	//
	// Example:
	//   // 10 KiB/s bandwidth, 10 MiB burst (no API rate limiting)
	//   ThrottleLayer throttle(10 * 1024, 10000 * 1024);
	ThrottleLayer(idx_t bandwidth_p, idx_t burst_p, shared_ptr<BaseClock> clock_p = nullptr);

	// Constructs a ThrottleLayer with both bandwidth and API rate limiting.
	// All parameters must be > 0, otherwise throws InvalidInputException.
	//
	// Example:
	//   // 10 KiB/s bandwidth, 10 MiB burst, 100 API calls/s
	//   ThrottleLayer throttle(10 * 1024, 10000 * 1024, 100);
	ThrottleLayer(idx_t bandwidth_p, idx_t burst_p, idx_t api_rate_p, shared_ptr<BaseClock> clock_p = nullptr);

	// Copy constructor. Creates a new ThrottleLayer sharing the same rate limiter state.
	// This means multiple copies will share the same bandwidth quota.
	ThrottleLayer(const ThrottleLayer &other);

	// Copy assignment operator.
	ThrottleLayer &operator=(const ThrottleLayer &other);

	// Move constructor.
	ThrottleLayer(ThrottleLayer &&other) noexcept;

	// Move assignment operator.
	ThrottleLayer &operator=(ThrottleLayer &&other) noexcept;

	// Destructor.
	~ThrottleLayer();

	// Reads data with bandwidth and API rate throttling.
	//
	// This method simulates a throttled read operation. It blocks until sufficient bandwidth
	// and API quota is available, then returns immediately (the actual read would be performed
	// by the inner operator in a real implementation).
	//
	// If bytes_to_read exceeds the burst size, the operation will fail with
	// ThrottleError::RequestExceedsBurst.
	//
	// Example:
	//   auto result = throttle.Read("/data/file.bin", 0, 4096);
	//   if (!result.success) {
	//       std::cerr << "Read failed: " << result.error_message << std::endl;
	//   }
	ReadResult Read(const string &path, idx_t start_offset, idx_t bytes_to_read);

	// Writes data with bandwidth and API rate throttling.
	//
	// This method applies bandwidth and API rate throttling before allowing a write operation.
	// It blocks until sufficient quota is available.
	//
	// If bytes_to_write exceeds the burst size, the operation will fail with
	// ThrottleError::RequestExceedsBurst.
	//
	// Example:
	//   auto result = throttle.Write("/data/output.bin", 8192);
	//   if (!result.success) {
	//       std::cerr << "Write failed: " << result.error_message << std::endl;
	//   }
	WriteResult Write(const string &path, idx_t bytes_to_write);

	// Returns the configured bandwidth in bytes per second.
	idx_t GetBandwidth() const;

	// Returns the configured burst size in bytes.
	idx_t GetBurst() const;

	// Returns the configured API rate limit (calls per second), or 0 if not configured.
	idx_t GetApiRate() const;

	// Returns true if API rate limiting is enabled.
	bool HasApiRateLimiting() const;

	// Returns the bandwidth rate limiter for advanced use cases.
	SharedRateLimiter GetBandwidthRateLimiter() const;

	// Returns the API rate limiter for advanced use cases, or nullptr if not configured.
	SharedRateLimiter GetApiRateLimiter() const;

private:
	// Configured bandwidth (bytes per second)
	idx_t bandwidth;
	// Configured burst size (bytes)
	idx_t burst;
	// Configured API rate (calls per second), 0 if disabled
	idx_t api_rate;
	// Shared rate limiter for bandwidth
	SharedRateLimiter bandwidth_limiter;
	// Shared rate limiter for API calls (nullptr if disabled)
	SharedRateLimiter api_limiter;
};

// Builder class for creating ThrottleLayer instances.
// Provides a fluent interface for configuring ThrottleLayer.
//
// Example:
//   auto throttle = ThrottleLayerBuilder()
//       .WithBandwidth(10 * 1024)     // 10 KiB/s
//       .WithBurst(10000 * 1024)      // 10 MiB
//       .WithApiRate(100)             // 100 calls/s
//       .Build();
class ThrottleLayerBuilder {
public:
	// Sets the bandwidth in bytes per second.
	ThrottleLayerBuilder &WithBandwidth(idx_t bandwidth_p) {
		bandwidth = bandwidth_p;
		return *this;
	}

	// Sets the burst size (maximum bytes at once).
	ThrottleLayerBuilder &WithBurst(idx_t burst_p) {
		burst = burst_p;
		return *this;
	}

	// Sets the API rate limit (maximum API calls per second).
	ThrottleLayerBuilder &WithApiRate(idx_t api_rate_p) {
		api_rate = api_rate_p;
		return *this;
	}

	// Sets a custom clock for testing purposes.
	ThrottleLayerBuilder &WithClock(shared_ptr<BaseClock> clock_p) {
		clock = clock_p;
		return *this;
	}

	// Builds the ThrottleLayer. Throws InvalidInputException if bandwidth or burst is not set or is 0.
	ThrottleLayer Build() const {
		if (bandwidth == 0) {
			throw InvalidInputException("bandwidth must be set and > 0");
		}
		if (burst == 0) {
			throw InvalidInputException("burst must be set and > 0");
		}
		if (api_rate > 0) {
			return ThrottleLayer(bandwidth, burst, api_rate, clock);
		}
		return ThrottleLayer(bandwidth, burst, clock);
	}

private:
	idx_t bandwidth = 0;
	idx_t burst = 0;
	idx_t api_rate = 0;
	shared_ptr<BaseClock> clock = nullptr;
};

} // namespace duckdb
