#include "rate_limiter.hpp"

#include "default_clock.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Quota
//===--------------------------------------------------------------------===//

Quota::Quota(idx_t bandwidth_p, idx_t burst_p) : bandwidth(bandwidth_p), burst(burst_p) {
	if (bandwidth_p == 0 && burst_p == 0) {
		throw InvalidInputException("at least one of bandwidth or burst must be greater than 0");
	}
}

idx_t Quota::GetBandwidth() const {
	return bandwidth;
}

idx_t Quota::GetBurst() const {
	return burst;
}

bool Quota::HasRateLimiting() const {
	return bandwidth > 0;
}

bool Quota::HasBurstLimiting() const {
	return burst > 0;
}

Duration Quota::GetEmissionInterval() const {
	if (bandwidth == 0) {
		return Duration::zero();
	}
	// Time per byte = 1 second / bandwidth
	return std::chrono::duration_cast<Duration>(std::chrono::seconds(1)) / bandwidth;
}

Duration Quota::GetDelayTolerance() const {
	if (bandwidth == 0 || burst == 0) {
		return Duration::max();
	}
	// Delay tolerance = burst * emission_interval
	auto emission_interval = GetEmissionInterval();
	return emission_interval * burst;
}

//===--------------------------------------------------------------------===//
// RateLimiterState
//===--------------------------------------------------------------------===//

RateLimiterState::RateLimiterState() : tat_nanos(0) {
}

int64_t RateLimiterState::GetTatNanos() const {
	return tat_nanos.load(std::memory_order_acquire);
}

bool RateLimiterState::CompareExchangeTat(int64_t &expected, int64_t desired) {
	return tat_nanos.compare_exchange_weak(expected, desired, std::memory_order_release, std::memory_order_relaxed);
}

//===--------------------------------------------------------------------===//
// RateLimiter
//===--------------------------------------------------------------------===//

RateLimiter::RateLimiter(const Quota &quota_p, shared_ptr<BaseClock> clock_p)
    : quota(quota_p), clock(clock_p ? clock_p : CreateDefaultClock()), state(make_uniq<RateLimiterState>()) {
}

shared_ptr<RateLimiter> RateLimiter::Direct(const Quota &quota_p, shared_ptr<BaseClock> clock_p) {
	return make_shared_ptr<RateLimiter>(quota_p, clock_p);
}

RateLimitResult RateLimiter::UntilNReady(idx_t n) {
	if (n == 0) {
		return RateLimitResult::Allowed;
	}

	// Check burst limit only if burst limiting is enabled
	if (quota.HasBurstLimiting() && n > quota.GetBurst()) {
		return RateLimitResult::InsufficientCapacity;
	}

	// If no rate limiting, always allowed immediately
	if (!quota.HasRateLimiting()) {
		return RateLimitResult::Allowed;
	}

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

std::optional<WaitInfo> RateLimiter::TryAcquireImmediate(idx_t n) {
	if (n == 0) {
		return std::nullopt;
	}

	// Check burst limit only if burst limiting is enabled
	if (quota.HasBurstLimiting() && n > quota.GetBurst()) {
		// Return max wait to indicate insufficient capacity
		return WaitInfo {TimePoint::max(), Duration::max()};
	}

	// If no rate limiting, always allowed immediately
	if (!quota.HasRateLimiting()) {
		return std::nullopt;
	}

	auto now = clock->Now();
	auto decision = TryAcquire(now, n);

	if (decision.allowed) {
		return std::nullopt;
	}
	return decision.wait_info;
}

const Quota &RateLimiter::GetQuota() const {
	return quota;
}

const shared_ptr<BaseClock> &RateLimiter::GetClock() const {
	return clock;
}

int64_t RateLimiter::ToNanos(TimePoint tp) {
	return std::chrono::duration_cast<Duration>(tp.time_since_epoch()).count();
}

TimePoint RateLimiter::FromNanos(int64_t nanos) {
	return TimePoint(Duration(nanos));
}

RateLimiter::AcquireDecision RateLimiter::TryAcquire(TimePoint now, idx_t n) {
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
			return AcquireDecision {false, WaitInfo {FromNanos(earliest_nanos), Duration(earliest_nanos - now_nanos)}};
		}

		// Try to update the TAT
		if (state->CompareExchangeTat(current_tat_nanos, new_tat_nanos)) {
			return AcquireDecision {true, std::nullopt};
		}
		// CAS failed, current_tat_nanos has been updated with current value, retry
	}
}

//===--------------------------------------------------------------------===//
// Helper function
//===--------------------------------------------------------------------===//

SharedRateLimiter CreateRateLimiter(idx_t bandwidth_p, idx_t burst_p, shared_ptr<BaseClock> clock_p) {
	Quota quota(bandwidth_p, burst_p);
	return RateLimiter::Direct(quota, clock_p);
}

} // namespace duckdb
