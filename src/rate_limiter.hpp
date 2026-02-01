#pragma once

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"

#include <cstdint>
#include <optional>

#include "base_clock.hpp"
#include "default_clock.hpp"

namespace duckdb {

/**
 * @class Quota
 * @brief Represents a rate limiting quota configuration.
 * @details Defines the bandwidth (bytes per second) and burst size
 *          (maximum bytes allowed at once) for rate limiting.
 */
class Quota {
public:
	/**
	 * @brief Create a per-second quota.
	 * @param bandwidth_p Maximum bytes allowed per second (must be > 0)
	 * @return Quota configured for per-second rate limiting
	 * @throws InvalidInputException if bandwidth is 0
	 */
	static Quota PerSecond(uint32_t bandwidth_p) {
		if (bandwidth_p == 0) {
			throw InvalidInputException("bandwidth must be greater than 0");
		}
		return Quota(bandwidth_p, bandwidth_p);
	}

	/**
	 * @brief Set the burst size for this quota.
	 * @param burst_p Maximum bytes allowed at once (must be > 0)
	 * @return Modified quota with the specified burst size
	 * @throws InvalidInputException if burst is 0
	 */
	Quota AllowBurst(uint32_t burst_p) const {
		if (burst_p == 0) {
			throw InvalidInputException("burst must be greater than 0");
		}
		return Quota(bandwidth, burst_p);
	}

	/**
	 * @brief Get the bandwidth (bytes per second).
	 */
	uint32_t GetBandwidth() const {
		return bandwidth;
	}

	/**
	 * @brief Get the burst size.
	 */
	uint32_t GetBurst() const {
		return burst;
	}

	/**
	 * @brief Calculate the emission interval (time between each byte).
	 * @return Duration representing time per byte
	 */
	Duration GetEmissionInterval() const {
		// Time per byte = 1 second / bandwidth
		return std::chrono::duration_cast<Duration>(std::chrono::seconds(1)) / bandwidth;
	}

	/**
	 * @brief Calculate the delay tolerance (maximum time that can be "borrowed").
	 * @return Duration representing the maximum delay tolerance
	 */
	Duration GetDelayTolerance() const {
		// Delay tolerance = burst * emission_interval
		auto emission_interval = GetEmissionInterval();
		return emission_interval * burst;
	}

private:
	Quota(uint32_t bandwidth_p, uint32_t burst_p) : bandwidth(bandwidth_p), burst(burst_p) {
	}

	uint32_t bandwidth;
	uint32_t burst;
};

/**
 * @class RateLimiterState
 * @brief Internal state for the GCRA rate limiter.
 * @details Manages the Theoretical Arrival Time (TAT) which represents
 *          when the next cell (byte) is expected to arrive according to
 *          the configured rate.
 */
class RateLimiterState {
public:
	RateLimiterState() : tat_nanos(0) {
	}

	/**
	 * @brief Get the current TAT as nanoseconds since epoch.
	 */
	int64_t GetTatNanos() const {
		return tat_nanos.load(std::memory_order_acquire);
	}

	/**
	 * @brief Compare and swap the TAT value atomically.
	 * @param expected Expected current value
	 * @param desired New value to set
	 * @return true if the swap succeeded, false otherwise
	 */
	bool CompareExchangeTat(int64_t &expected, int64_t desired) {
		return tat_nanos.compare_exchange_weak(expected, desired, std::memory_order_release, std::memory_order_relaxed);
	}

private:
	atomic<int64_t> tat_nanos;
};

/**
 * @brief Result of a rate limiting decision.
 */
enum class RateLimitResult {
	/// The request is allowed to proceed
	Allowed,
	/// The request exceeds the burst capacity
	InsufficientCapacity,
};

/**
 * @brief Information about when to retry after rate limiting.
 */
struct WaitInfo {
	/// The time point at which the request can be retried
	TimePoint ready_at;
	/// Duration to wait from now
	Duration wait_duration;
};

/**
 * @class RateLimiter
 * @brief Thread-safe GCRA (Generic Cell Rate Algorithm) rate limiter.
 * @details Implements a leaky bucket algorithm variant that provides
 *          smooth rate limiting with burst support. The algorithm:
 *          - Tracks a Theoretical Arrival Time (TAT) for the next allowed request
 *          - Allows bursting up to the configured burst size
 *          - Provides nanosecond precision timing
 *          - Is lock-free for the common case (using atomic operations)
 *
 * @see https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm
 */
class RateLimiter {
public:
	/**
	 * @brief Create a rate limiter with the specified quota.
	 * @param quota_p The rate limiting quota (bandwidth + burst)
	 * @param clock_p Optional clock implementation (defaults to system clock)
	 */
	explicit RateLimiter(const Quota &quota_p, shared_ptr<BaseClock> clock_p = nullptr)
	    : quota(quota_p), clock(clock_p ? clock_p : CreateDefaultClock()), state(make_uniq<RateLimiterState>()) {
	}

	/**
	 * @brief Create a direct rate limiter (alias for the constructor).
	 * @param quota_p The rate limiting quota
	 * @param clock_p Optional clock implementation
	 * @return Shared pointer to the rate limiter
	 */
	static shared_ptr<RateLimiter> Direct(const Quota &quota_p, shared_ptr<BaseClock> clock_p = nullptr) {
		return make_shared_ptr<RateLimiter>(quota_p, clock_p);
	}

	/**
	 * @brief Check if n bytes can be transmitted now without waiting.
	 * @param n Number of bytes to check
	 * @return RateLimitResult::Allowed if allowed, or InsufficientCapacity if
	 *         n exceeds burst
	 */
	RateLimitResult Check(uint32_t n) const {
		if (n == 0)
			return RateLimitResult::Allowed;
		if (n > quota.GetBurst())
			return RateLimitResult::InsufficientCapacity;

		auto now = clock->Now();
		auto wait_info = CheckAt(now, n);

		if (wait_info && wait_info->wait_duration > Duration::zero()) {
			return RateLimitResult::Allowed; // Would need to wait but quota is valid
		}
		return RateLimitResult::Allowed;
	}

	/**
	 * @brief Wait until n bytes can be transmitted.
	 * @param n Number of bytes to transmit (must be <= burst)
	 * @return RateLimitResult::Allowed on success, InsufficientCapacity if
	 *         n > burst
	 */
	RateLimitResult UntilNReady(uint32_t n) {
		if (n == 0)
			return RateLimitResult::Allowed;
		if (n > quota.GetBurst())
			return RateLimitResult::InsufficientCapacity;

		while (true) {
			auto now = clock->Now();
			auto decision = TryAcquire(now, n);

			if (decision.allowed) {
				return RateLimitResult::Allowed;
			}

			// Wait until we can proceed
			if (decision.wait_info) {
				clock->SleepUntil(decision.wait_info->ready_at);
			}
		}
	}

	/**
	 * @brief Try to acquire permission for n bytes without waiting.
	 * @param n Number of bytes to acquire
	 * @return Optional WaitInfo if waiting is required, nullopt if allowed
	 *         immediately
	 */
	std::optional<WaitInfo> TryAcquireImmediate(uint32_t n) {
		if (n == 0)
			return std::nullopt;
		if (n > quota.GetBurst()) {
			// Return max wait to indicate insufficient capacity
			return WaitInfo {TimePoint::max(), Duration::max()};
		}

		auto now = clock->Now();
		auto decision = TryAcquire(now, n);

		if (decision.allowed) {
			return std::nullopt;
		}
		return decision.wait_info;
	}

	/**
	 * @brief Get the configured quota.
	 */
	const Quota &GetQuota() const {
		return quota;
	}

	/**
	 * @brief Get the clock used by this rate limiter.
	 */
	const shared_ptr<BaseClock> &GetClock() const {
		return clock;
	}

private:
	struct AcquireDecision {
		bool allowed;
		std::optional<WaitInfo> wait_info;
	};

	/**
	 * @brief Convert a TimePoint to nanoseconds since epoch.
	 */
	static int64_t ToNanos(TimePoint tp) {
		return std::chrono::duration_cast<Duration>(tp.time_since_epoch()).count();
	}

	/**
	 * @brief Convert nanoseconds since epoch to a TimePoint.
	 */
	static TimePoint FromNanos(int64_t nanos) {
		return TimePoint(Duration(nanos));
	}

	/**
	 * @brief Check rate limit at a specific time point.
	 */
	std::optional<WaitInfo> CheckAt(TimePoint now, uint32_t n) const {
		auto emission_interval = quota.GetEmissionInterval();
		auto delay_tolerance = quota.GetDelayTolerance();

		int64_t now_nanos = ToNanos(now);
		int64_t current_tat_nanos = state->GetTatNanos();

		// Calculate the increment for n bytes
		auto increment = emission_interval * n;
		int64_t increment_nanos = std::chrono::duration_cast<Duration>(increment).count();

		// Calculate the new TAT
		int64_t new_tat_nanos = std::max(current_tat_nanos, now_nanos) + increment_nanos;

		// Calculate the earliest time this request could complete
		int64_t delay_tolerance_nanos = std::chrono::duration_cast<Duration>(delay_tolerance).count();
		int64_t earliest_nanos = new_tat_nanos - delay_tolerance_nanos;

		if (earliest_nanos > now_nanos) {
			// Need to wait
			return WaitInfo {FromNanos(earliest_nanos), Duration(earliest_nanos - now_nanos)};
		}

		return std::nullopt;
	}

	/**
	 * @brief Try to acquire rate limit at a specific time point.
	 */
	AcquireDecision TryAcquire(TimePoint now, uint32_t n) {
		auto emission_interval = quota.GetEmissionInterval();
		auto delay_tolerance = quota.GetDelayTolerance();

		int64_t now_nanos = ToNanos(now);

		// Calculate the increment for n bytes
		auto increment = emission_interval * n;
		int64_t increment_nanos = std::chrono::duration_cast<Duration>(increment).count();
		int64_t delay_tolerance_nanos = std::chrono::duration_cast<Duration>(delay_tolerance).count();

		// Atomic CAS loop
		int64_t current_tat_nanos = state->GetTatNanos();
		while (true) {
			// Calculate the new TAT
			int64_t new_tat_nanos = std::max(current_tat_nanos, now_nanos) + increment_nanos;

			// Calculate the earliest time this request could complete
			int64_t earliest_nanos = new_tat_nanos - delay_tolerance_nanos;

			if (earliest_nanos > now_nanos) {
				// Need to wait - don't update state, just return wait info
				return AcquireDecision {false,
				                        WaitInfo {FromNanos(earliest_nanos), Duration(earliest_nanos - now_nanos)}};
			}

			// Try to update the TAT
			if (state->CompareExchangeTat(current_tat_nanos, new_tat_nanos)) {
				return AcquireDecision {true, std::nullopt};
			}
			// CAS failed, current_tat_nanos has been updated with current value, retry
		}
	}

	Quota quota;
	shared_ptr<BaseClock> clock;
	unique_ptr<RateLimiterState> state;
};

/**
 * @brief Shared rate limiter type for thread-safe access across multiple
 *        threads/operations.
 */
using SharedRateLimiter = shared_ptr<RateLimiter>;

/**
 * @brief Create a direct rate limiter with the specified bandwidth and burst.
 * @param bandwidth_p Maximum bytes per second
 * @param burst_p Maximum bytes allowed at once
 * @param clock_p Optional clock implementation
 * @return Shared pointer to the rate limiter
 */
inline SharedRateLimiter CreateRateLimiter(uint32_t bandwidth_p, uint32_t burst_p,
                                           shared_ptr<BaseClock> clock_p = nullptr) {
	auto quota = Quota::PerSecond(bandwidth_p).AllowBurst(burst_p);
	return RateLimiter::Direct(quota, clock_p);
}

} // namespace duckdb
