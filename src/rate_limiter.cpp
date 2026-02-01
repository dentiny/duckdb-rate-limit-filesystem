#include "rate_limiter.hpp"

#include "default_clock.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Quota
//===--------------------------------------------------------------------===//

Quota::Quota(idx_t bandwidth_p, idx_t burst_p) : bandwidth(bandwidth_p), burst(burst_p) {
}

Quota Quota::PerSecond(idx_t bandwidth_p) {
	if (bandwidth_p == 0) {
		throw InvalidInputException("bandwidth must be greater than 0");
	}
	return Quota(bandwidth_p, bandwidth_p);
}

Quota Quota::AllowBurst(idx_t burst_p) const {
	if (burst_p == 0) {
		throw InvalidInputException("burst must be greater than 0");
	}
	return Quota(bandwidth, burst_p);
}

idx_t Quota::GetBandwidth() const {
	return bandwidth;
}

idx_t Quota::GetBurst() const {
	return burst;
}

Duration Quota::GetEmissionInterval() const {
	// Time per byte = 1 second / bandwidth
	return std::chrono::duration_cast<Duration>(std::chrono::seconds(1)) / bandwidth;
}

Duration Quota::GetDelayTolerance() const {
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

RateLimitResult RateLimiter::Check(idx_t n) const {
	if (n == 0) {
		return RateLimitResult::Allowed;
	}
	if (n > quota.GetBurst()) {
		return RateLimitResult::InsufficientCapacity;
	}

	auto now = clock->Now();
	auto wait_info = CheckAt(now, n);

	if (wait_info && wait_info->wait_duration > Duration::zero()) {
		return RateLimitResult::Allowed; // Would need to wait but quota is valid
	}
	return RateLimitResult::Allowed;
}

RateLimitResult RateLimiter::UntilNReady(idx_t n) {
	if (n == 0) {
		return RateLimitResult::Allowed;
	}
	if (n > quota.GetBurst()) {
		return RateLimitResult::InsufficientCapacity;
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

std::optional<WaitInfo> RateLimiter::CheckAt(TimePoint now, idx_t n) const {
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
	auto quota = Quota::PerSecond(bandwidth_p).AllowBurst(burst_p);
	return RateLimiter::Direct(quota, clock_p);
}

} // namespace duckdb
