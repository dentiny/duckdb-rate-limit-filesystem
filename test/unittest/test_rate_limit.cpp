#include "catch/catch.hpp"

#include "mock_clock.hpp"
#include "rate_limiter.hpp"
#include "throttle_layer.hpp"

using namespace duckdb;

TEST_CASE("Rate limit - first request within burst passes immediately", "[rate]") {
	auto clock = CreateMockClock();
	// 100 bytes/sec, 100 byte burst
	auto quota = Quota::PerSecond(100).AllowBurst(100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// First request should pass immediately
	auto wait_info = limiter->TryAcquireImmediate(50);
	REQUIRE_FALSE(wait_info.has_value());
}

TEST_CASE("Rate limit - consecutive requests require waiting", "[rate]") {
	auto clock = CreateMockClock();
	// 100 bytes/sec, 100 byte burst
	// This means: 1 byte takes 10ms (1000ms / 100 bytes)
	auto quota = Quota::PerSecond(100).AllowBurst(100);
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
	// 100 bytes/sec, 100 byte burst
	auto quota = Quota::PerSecond(100).AllowBurst(100);
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
	// 100 bytes/sec, 100 byte burst
	auto quota = Quota::PerSecond(100).AllowBurst(100);
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
	// 100 bytes/sec, 100 byte burst
	auto quota = Quota::PerSecond(100).AllowBurst(100);
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
	// 100 bytes/sec, 100 byte burst
	auto quota = Quota::PerSecond(100).AllowBurst(100);
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

TEST_CASE("Rate limit - ThrottleLayer respects rate limit over time", "[rate][throttle]") {
	auto clock = CreateMockClock();
	// 100 bytes/sec, 100 byte burst
	ThrottleLayer throttle(100, 100, clock);

	TimePoint start_time = clock->Now();

	// Make 3 requests of 100 bytes each
	// First should pass immediately (uses burst)
	// Second and third should advance time
	for (int i = 0; i < 3; i++) {
		auto result = throttle.Read("/test/file", 0, 100);
		REQUIRE(result.success);
	}

	TimePoint end_time = clock->Now();

	// Should have taken ~2 seconds (first uses burst, 2nd and 3rd need 1s each)
	auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);
	REQUIRE(elapsed.count() >= 2);
}

TEST_CASE("Rate limit - API rate limiting works independently", "[rate][throttle][api]") {
	auto clock = CreateMockClock();
	// 10000 bytes/sec bandwidth, 10000 byte burst, 2 API calls/sec
	ThrottleLayer throttle(10000, 10000, 2, clock);

	TimePoint start_time = clock->Now();

	// Make 4 small API calls
	// First 2 should pass immediately (API burst = rate = 2)
	// 3rd and 4th should need to wait
	for (int i = 0; i < 4; i++) {
		auto result = throttle.Read("/test/file", 0, 10);
		REQUIRE(result.success);
	}

	TimePoint end_time = clock->Now();

	// Should have taken ~1 second (first 2 use burst, 3rd and 4th need 0.5s each)
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	REQUIRE(elapsed.count() >= 1000);
}

TEST_CASE("Rate limit - emission interval calculation", "[rate][quota]") {
	// 1000 bytes/sec -> 1ms per byte
	auto quota = Quota::PerSecond(1000).AllowBurst(100);

	auto emission_interval = quota.GetEmissionInterval();
	auto expected = std::chrono::duration_cast<Duration>(std::chrono::milliseconds(1));

	REQUIRE(emission_interval == expected);
}

TEST_CASE("Rate limit - delay tolerance calculation", "[rate][quota]") {
	// 1000 bytes/sec, 100 byte burst -> delay tolerance = 100ms
	auto quota = Quota::PerSecond(1000).AllowBurst(100);

	auto delay_tolerance = quota.GetDelayTolerance();
	auto expected = std::chrono::duration_cast<Duration>(std::chrono::milliseconds(100));

	REQUIRE(delay_tolerance == expected);
}

TEST_CASE("Rate limit - high bandwidth low burst scenario", "[rate]") {
	auto clock = CreateMockClock();
	// 10000 bytes/sec, 100 byte burst
	// Can only do 100 byte requests at a time, but they process quickly
	auto quota = Quota::PerSecond(10000).AllowBurst(100);
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
	// 10 bytes/sec, 1000 byte burst
	// Can do large requests but they take a long time to replenish
	auto quota = Quota::PerSecond(10).AllowBurst(1000);
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
	// 1000 bytes/sec, 500 byte burst
	auto quota = Quota::PerSecond(1000).AllowBurst(500);
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

TEST_CASE("Rate limit - ThrottleLayer builder with rate limiting", "[rate][throttle][builder]") {
	auto clock = CreateMockClock();

	auto throttle = ThrottleLayerBuilder()
	                    .WithBandwidth(100)
	                    .WithBurst(100)
	                    .WithApiRate(10)
	                    .WithClock(clock)
	                    .Build();

	REQUIRE(throttle.GetBandwidth() == 100);
	REQUIRE(throttle.GetBurst() == 100);
	REQUIRE(throttle.GetApiRate() == 10);
	REQUIRE(throttle.HasApiRateLimiting());
}

TEST_CASE("Rate limit - ThrottleLayer without API rate limiting", "[rate][throttle]") {
	auto clock = CreateMockClock();

	ThrottleLayer throttle(100, 100, clock);

	REQUIRE(throttle.GetBandwidth() == 100);
	REQUIRE(throttle.GetBurst() == 100);
	REQUIRE(throttle.GetApiRate() == 0);
	REQUIRE_FALSE(throttle.HasApiRateLimiting());
}

TEST_CASE("Rate limit - GetBandwidthRateLimiter returns valid limiter", "[rate][throttle]") {
	auto clock = CreateMockClock();
	ThrottleLayer throttle(100, 100, clock);

	auto bw_limiter = throttle.GetBandwidthRateLimiter();
	REQUIRE(bw_limiter != nullptr);
	REQUIRE(bw_limiter->GetQuota().GetBandwidth() == 100);
	REQUIRE(bw_limiter->GetQuota().GetBurst() == 100);
}

TEST_CASE("Rate limit - GetApiRateLimiter returns nullptr when not configured", "[rate][throttle]") {
	auto clock = CreateMockClock();
	ThrottleLayer throttle(100, 100, clock);

	auto api_limiter = throttle.GetApiRateLimiter();
	REQUIRE(api_limiter == nullptr);
}

TEST_CASE("Rate limit - GetApiRateLimiter returns valid limiter when configured", "[rate][throttle]") {
	auto clock = CreateMockClock();
	ThrottleLayer throttle(100, 100, 10, clock);

	auto api_limiter = throttle.GetApiRateLimiter();
	REQUIRE(api_limiter != nullptr);
	REQUIRE(api_limiter->GetQuota().GetBandwidth() == 10);
}

