#include "catch/catch.hpp"

#include "mock_clock.hpp"
#include "rate_limiter.hpp"

using namespace duckdb;

TEST_CASE("Rate limit - first request within burst passes immediately", "[rate]") {
	auto clock = CreateMockClock();
	Quota quota(/*bandwidth_p=*/100, /*burst_p=*/100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// First request should pass immediately
	auto wait_info = limiter->TryAcquireImmediate(50);
	REQUIRE_FALSE(wait_info.has_value());
}

TEST_CASE("Rate limit - consecutive requests require waiting", "[rate]") {
	auto clock = CreateMockClock();
	// 1 byte takes 10ms (1000ms / 100 bytes)
	Quota quota(/*bandwidth_p=*/100, /*burst_p=*/100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// First request for 100 bytes (uses full burst)
	auto result1 = limiter->UntilNReady(100);
	REQUIRE(result1 == RateLimitResult::Allowed);

	// Second request should require waiting
	auto wait_info = limiter->TryAcquireImmediate(100);
	REQUIRE(wait_info.has_value());
	REQUIRE(wait_info->wait_duration > Duration::zero());
}

TEST_CASE("Rate limit - quota replenishes over time", "[rate]") {
	auto clock = CreateMockClock();
	Quota quota(/*bandwidth_p=*/100, /*burst_p=*/100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Use full burst
	auto result1 = limiter->UntilNReady(100);
	REQUIRE(result1 == RateLimitResult::Allowed);

	// Immediately after, need to wait
	auto wait_info = limiter->TryAcquireImmediate(100);
	REQUIRE(wait_info.has_value());

	// Advance time by 1 second (should replenish 100 bytes)
	clock->Advance(std::chrono::seconds(1));

	// Now should be able to acquire again
	auto wait_info2 = limiter->TryAcquireImmediate(100);
	REQUIRE_FALSE(wait_info2.has_value());
}

TEST_CASE("Rate limit - partial quota replenishment", "[rate]") {
	auto clock = CreateMockClock();
	Quota quota(/*bandwidth_p=*/100, /*burst_p=*/100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Use full burst
	limiter->UntilNReady(100);

	// Advance time by 500ms (should replenish 50 bytes)
	clock->Advance(std::chrono::milliseconds(500));

	// Should be able to acquire 50 bytes
	auto wait_info = limiter->TryAcquireImmediate(50);
	REQUIRE_FALSE(wait_info.has_value());
}

TEST_CASE("Rate limit - UntilNReady blocks and advances mock clock", "[rate]") {
	auto clock = CreateMockClock();
	Quota quota(/*bandwidth_p=*/100, /*burst_p=*/100);
	auto limiter = RateLimiter::Direct(quota, clock);

	TimePoint start_time = clock->Now();

	// Use full burst
	limiter->UntilNReady(100);

	// Second request will block (advancing mock clock via SleepUntil)
	limiter->UntilNReady(100);

	TimePoint end_time = clock->Now();

	// Time should have advanced by approximately 1 second (100 bytes at 100 bytes/sec)
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	REQUIRE(elapsed.count() >= 1000);
}

TEST_CASE("Rate limit - small requests accumulate correctly", "[rate]") {
	auto clock = CreateMockClock();
	Quota quota(/*bandwidth_p=*/100, /*burst_p=*/100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Make 10 requests of 10 bytes each (total 100 bytes = full burst)
	for (int i = 0; i < 10; i++) {
		auto result = limiter->UntilNReady(10);
		REQUIRE(result == RateLimitResult::Allowed);
	}

	// 11th request should require waiting
	auto wait_info = limiter->TryAcquireImmediate(10);
	REQUIRE(wait_info.has_value());
}

TEST_CASE("Rate limit - emission interval calculation", "[rate][quota]") {
	// 1000 bytes/sec -> 1ms per byte
	Quota quota(/*bandwidth_p=*/1000, /*burst_p=*/100);

	auto emission_interval = quota.GetEmissionInterval();
	auto expected = std::chrono::duration_cast<Duration>(std::chrono::milliseconds(1));

	REQUIRE(emission_interval == expected);
}

TEST_CASE("Rate limit - delay tolerance calculation", "[rate][quota]") {
	// 1000 bytes/sec, 100 byte burst -> delay tolerance = 100ms
	Quota quota(/*bandwidth_p=*/1000, /*burst_p=*/100);

	auto delay_tolerance = quota.GetDelayTolerance();
	auto expected = std::chrono::duration_cast<Duration>(std::chrono::milliseconds(100));

	REQUIRE(delay_tolerance == expected);
}

TEST_CASE("Rate limit - high bandwidth low burst scenario", "[rate]") {
	auto clock = CreateMockClock();
	// Can only do 100 byte requests at a time, but they process quickly
	Quota quota(/*bandwidth_p=*/10000, /*burst_p=*/100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// First request passes
	limiter->UntilNReady(100);

	// Check how long until next 100 bytes
	auto wait_info = limiter->TryAcquireImmediate(100);
	REQUIRE(wait_info.has_value());

	// Should only need to wait 10ms (100 bytes at 10000 bytes/sec)
	auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(wait_info->wait_duration);
	REQUIRE(wait_ms.count() == 10);
}

TEST_CASE("Rate limit - low bandwidth high burst scenario", "[rate]") {
	auto clock = CreateMockClock();
	// Can do large requests but they take a long time to replenish
	Quota quota(/*bandwidth_p=*/10, /*burst_p=*/1000);
	auto limiter = RateLimiter::Direct(quota, clock);

	// First large request passes (uses burst capacity)
	limiter->UntilNReady(1000);

	// Check how long until next 1000 bytes (full burst)
	auto wait_info = limiter->TryAcquireImmediate(1000);
	REQUIRE(wait_info.has_value());

	// Should need to wait 100 seconds (1000 bytes at 10 bytes/sec)
	auto wait_sec = std::chrono::duration_cast<std::chrono::seconds>(wait_info->wait_duration);
	REQUIRE(wait_sec.count() == 100);
}

TEST_CASE("Rate limit - concurrent-style requests with mock clock", "[rate]") {
	auto clock = CreateMockClock();
	Quota quota(/*bandwidth_p=*/1000, /*burst_p=*/500);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Request 1: 200 bytes
	limiter->UntilNReady(200);

	// Request 2: 200 bytes (should still fit in burst)
	auto wait_info1 = limiter->TryAcquireImmediate(200);
	REQUIRE_FALSE(wait_info1.has_value());
	limiter->UntilNReady(200);

	// Request 3: 200 bytes (exceeds burst, need to wait)
	auto wait_info2 = limiter->TryAcquireImmediate(200);
	REQUIRE(wait_info2.has_value());

	// Advance time by wait duration
	clock->Advance(wait_info2->wait_duration);

	// Now should be able to acquire
	auto wait_info3 = limiter->TryAcquireImmediate(200);
	REQUIRE_FALSE(wait_info3.has_value());
}

TEST_CASE("Rate limit - CreateRateLimiter helper function", "[rate]") {
	auto clock = CreateMockClock();

	auto limiter = CreateRateLimiter(/*bandwidth_p=*/100, /*burst_p=*/100, clock);

	REQUIRE(limiter != nullptr);
	REQUIRE(limiter->GetQuota().GetBandwidth() == 100);
	REQUIRE(limiter->GetQuota().GetBurst() == 100);
}

TEST_CASE("Rate limit - no rate limiting mode passes immediately", "[rate]") {
	auto clock = CreateMockClock();
	// No rate limiting (bandwidth=0), only burst limiting
	Quota quota(/*bandwidth_p=*/0, /*burst_p=*/100);
	auto limiter = RateLimiter::Direct(quota, clock);

	TimePoint start_time = clock->Now();

	// Multiple requests should pass immediately without advancing clock
	for (int i = 0; i < 100; i++) {
		auto result = limiter->UntilNReady(50);
		REQUIRE(result == RateLimitResult::Allowed);
	}

	TimePoint end_time = clock->Now();

	// No time should have passed (no rate limiting)
	REQUIRE(start_time == end_time);
}

TEST_CASE("Rate limit - no burst limiting mode allows large requests", "[rate]") {
	auto clock = CreateMockClock();
	// No burst limiting (burst=0), only rate limiting
	Quota quota(/*bandwidth_p=*/1000, /*burst_p=*/0);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Very large request should pass (no burst limit)
	auto result = limiter->UntilNReady(1000000);
	REQUIRE(result == RateLimitResult::Allowed);
}

TEST_CASE("Rate limit - emission interval is zero when no rate limiting", "[rate][quota]") {
	Quota quota(/*bandwidth_p=*/0, /*burst_p=*/100);

	auto emission_interval = quota.GetEmissionInterval();
	REQUIRE(emission_interval == Duration::zero());
}

TEST_CASE("Rate limit - delay tolerance for special cases", "[rate][quota]") {
	// No burst limiting (rate-only) - uses emission_interval (allows 1 op immediately)
	// 1000 bytes/sec -> 1ms per byte -> delay tolerance = 1ms
	Quota quota1(/*bandwidth_p=*/1000, /*burst_p=*/0);
	auto expected = std::chrono::duration_cast<Duration>(std::chrono::milliseconds(1));
	REQUIRE(quota1.GetDelayTolerance() == expected);

	// No rate limiting - infinite tolerance
	Quota quota2(/*bandwidth_p=*/0, /*burst_p=*/100);
	REQUIRE(quota2.GetDelayTolerance() == Duration::max());
}
