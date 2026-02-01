#include "catch/catch.hpp"

#include "mock_clock.hpp"
#include "rate_limiter.hpp"

using namespace duckdb;

TEST_CASE("Burst limit - request within burst passes", "[burst]") {
	// Create a rate limiter with 1000 bytes/sec bandwidth and 100 byte burst
	auto clock = CreateMockClock();
	Quota quota(1000, 100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Request exactly at burst limit should pass
	auto result = limiter->UntilNReady(100);
	REQUIRE(result == RateLimitResult::Allowed);
}

TEST_CASE("Burst limit - request below burst passes", "[burst]") {
	auto clock = CreateMockClock();
	Quota quota(1000, 100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Request below burst limit should pass
	auto result = limiter->UntilNReady(50);
	REQUIRE(result == RateLimitResult::Allowed);
}

TEST_CASE("Burst limit - request exceeding burst fails immediately", "[burst]") {
	auto clock = CreateMockClock();
	Quota quota(1000, 100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Request exceeding burst limit should fail with InsufficientCapacity
	auto result = limiter->UntilNReady(101);
	REQUIRE(result == RateLimitResult::InsufficientCapacity);
}

TEST_CASE("Burst limit - zero byte request always passes", "[burst]") {
	auto clock = CreateMockClock();
	Quota quota(1000, 100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Zero-byte request should always pass
	auto result = limiter->UntilNReady(0);
	REQUIRE(result == RateLimitResult::Allowed);
}

TEST_CASE("Burst limit - Check method returns InsufficientCapacity for oversized request", "[burst]") {
	auto clock = CreateMockClock();
	Quota quota(1000, 100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Check should return InsufficientCapacity for request exceeding burst
	auto result = limiter->Check(200);
	REQUIRE(result == RateLimitResult::InsufficientCapacity);
}

TEST_CASE("Burst limit - TryAcquireImmediate returns max wait for oversized request", "[burst]") {
	auto clock = CreateMockClock();
	Quota quota(1000, 100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// TryAcquireImmediate should return max duration for request exceeding burst
	auto wait_info = limiter->TryAcquireImmediate(200);
	REQUIRE(wait_info.has_value());
	REQUIRE(wait_info->wait_duration == Duration::max());
}

TEST_CASE("Burst limit - Quota creation with zero bandwidth throws", "[burst][quota]") {
	REQUIRE_THROWS_AS(Quota(0, 100), InvalidInputException);
}

TEST_CASE("Burst limit - Quota creation with zero burst throws", "[burst][quota]") {
	REQUIRE_THROWS_AS(Quota(1000, 0), InvalidInputException);
}

TEST_CASE("Burst limit - Quota getters return correct values", "[burst][quota]") {
	Quota quota(1000, 100);
	REQUIRE(quota.GetBandwidth() == 1000);
	REQUIRE(quota.GetBurst() == 100);
}

TEST_CASE("Burst limit - multiple small requests within burst", "[burst]") {
	auto clock = CreateMockClock();
	Quota quota(1000, 100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// 10 requests of 10 bytes each should all pass (100 bytes total = burst)
	for (int i = 0; i < 10; i++) {
		auto result = limiter->UntilNReady(10);
		REQUIRE(result == RateLimitResult::Allowed);
	}
}

TEST_CASE("Burst limit - RateLimiter::Direct creates shared pointer", "[burst]") {
	auto clock = CreateMockClock();
	Quota quota(1000, 100);
	auto limiter = RateLimiter::Direct(quota, clock);

	REQUIRE(limiter != nullptr);
	REQUIRE(limiter->GetQuota().GetBandwidth() == 1000);
	REQUIRE(limiter->GetQuota().GetBurst() == 100);
}
