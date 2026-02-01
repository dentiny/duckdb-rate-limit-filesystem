#pragma once

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"

#include <optional>

#include "base_clock.hpp"

namespace duckdb {

// Forward declaration
shared_ptr<BaseClock> CreateDefaultClock();

// Represents a rate limiting quota configuration.
// Defines the bandwidth (bytes per second) and burst size (maximum bytes allowed at once).
class Quota {
public:
	// Creates a per-second quota with the specified bandwidth.
	// Throws InvalidInputException if bandwidth is 0.
	static Quota PerSecond(idx_t bandwidth_p);

	// Sets the burst size for this quota. Returns a modified quota with the specified burst size.
	// Throws InvalidInputException if burst is 0.
	Quota AllowBurst(idx_t burst_p) const;

	// Returns the bandwidth in bytes per second.
	idx_t GetBandwidth() const;

	// Returns the burst size in bytes.
	idx_t GetBurst() const;

	// Returns the emission interval (time between each byte).
	Duration GetEmissionInterval() const;

	// Returns the delay tolerance (maximum time that can be "borrowed").
	Duration GetDelayTolerance() const;

private:
	Quota(idx_t bandwidth_p, idx_t burst_p);

	idx_t bandwidth;
	idx_t burst;
};

// Internal state for the GCRA rate limiter.
// Manages the Theoretical Arrival Time (TAT) which represents when the next cell (byte)
// is expected to arrive according to the configured rate.
class RateLimiterState {
public:
	RateLimiterState();

	// Returns the current TAT as nanoseconds since epoch.
	int64_t GetTatNanos() const;

	// Atomically compares and swaps the TAT value. Returns true if the swap succeeded.
	bool CompareExchangeTat(int64_t &expected, int64_t desired);

private:
	atomic<int64_t> tat_nanos;
};

// Result of a rate limiting decision.
enum class RateLimitResult {
	// The request is allowed to proceed
	Allowed,
	// The request exceeds the burst capacity
	InsufficientCapacity,
};

// Information about when to retry after rate limiting.
struct WaitInfo {
	// The time point at which the request can be retried
	TimePoint ready_at;
	// Duration to wait from now
	Duration wait_duration;
};

// Thread-safe GCRA (Generic Cell Rate Algorithm) rate limiter.
//
// Implements a leaky bucket algorithm variant that provides smooth rate limiting with burst support.
// The algorithm:
// - Tracks a Theoretical Arrival Time (TAT) for the next allowed request
// - Allows bursting up to the configured burst size
// - Provides nanosecond precision timing
// - Is lock-free for the common case (using atomic operations)
//
// See https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm
class RateLimiter {
public:
	// Creates a rate limiter with the specified quota and optional clock implementation.
	explicit RateLimiter(const Quota &quota_p, shared_ptr<BaseClock> clock_p = nullptr);

	// Creates a shared rate limiter with the specified quota and optional clock.
	static shared_ptr<RateLimiter> Direct(const Quota &quota_p, shared_ptr<BaseClock> clock_p = nullptr);

	// Checks if n bytes can be transmitted now without waiting.
	// Returns Allowed if allowed, or InsufficientCapacity if n exceeds burst.
	RateLimitResult Check(idx_t n) const;

	// Waits until n bytes can be transmitted. The request must be <= burst size.
	// Returns Allowed on success, InsufficientCapacity if n > burst.
	RateLimitResult UntilNReady(idx_t n);

	// Tries to acquire permission for n bytes without waiting.
	// Returns WaitInfo if waiting is required, nullopt if allowed immediately.
	std::optional<WaitInfo> TryAcquireImmediate(idx_t n);

	// Returns the configured quota.
	const Quota &GetQuota() const;

	// Returns the clock used by this rate limiter.
	const shared_ptr<BaseClock> &GetClock() const;

private:
	struct AcquireDecision {
		bool allowed;
		std::optional<WaitInfo> wait_info;
	};

	// Converts a TimePoint to nanoseconds since epoch.
	static int64_t ToNanos(TimePoint tp);

	// Converts nanoseconds since epoch to a TimePoint.
	static TimePoint FromNanos(int64_t nanos);

	// Checks rate limit at a specific time point.
	std::optional<WaitInfo> CheckAt(TimePoint now, idx_t n) const;

	// Tries to acquire rate limit at a specific time point.
	AcquireDecision TryAcquire(TimePoint now, idx_t n);

	Quota quota;
	shared_ptr<BaseClock> clock;
	unique_ptr<RateLimiterState> state;
};

// Shared rate limiter type for thread-safe access across multiple threads/operations.
using SharedRateLimiter = shared_ptr<RateLimiter>;

// Creates a shared rate limiter with the specified bandwidth, burst, and optional clock.
SharedRateLimiter CreateRateLimiter(idx_t bandwidth_p, idx_t burst_p, shared_ptr<BaseClock> clock_p = nullptr);

} // namespace duckdb
