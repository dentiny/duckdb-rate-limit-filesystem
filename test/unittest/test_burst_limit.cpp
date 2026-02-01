#include "catch/catch.hpp"

#include "mock_clock.hpp"
#include "rate_limiter.hpp"
#include "throttle_layer.hpp"

using namespace duckdb;

TEST_CASE("Burst limit - request within burst passes", "[burst]") {
	// Create a rate limiter with 1000 bytes/sec bandwidth and 100 byte burst
	auto clock = CreateMockClock();
	auto quota = Quota::PerSecond(1000).AllowBurst(100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Request exactly at burst limit should pass
	auto result = limiter->UntilNReady(100);
	REQUIRE(result == RateLimitResult::Allowed);
}

TEST_CASE("Burst limit - request below burst passes", "[burst]") {
	auto clock = CreateMockClock();
	auto quota = Quota::PerSecond(1000).AllowBurst(100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Request below burst limit should pass
	auto result = limiter->UntilNReady(50);
	REQUIRE(result == RateLimitResult::Allowed);
}

TEST_CASE("Burst limit - request exceeding burst fails immediately", "[burst]") {
	auto clock = CreateMockClock();
	auto quota = Quota::PerSecond(1000).AllowBurst(100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Request exceeding burst limit should fail with InsufficientCapacity
	auto result = limiter->UntilNReady(101);
	REQUIRE(result == RateLimitResult::InsufficientCapacity);
}

TEST_CASE("Burst limit - zero byte request always passes", "[burst]") {
	auto clock = CreateMockClock();
	auto quota = Quota::PerSecond(1000).AllowBurst(100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Zero-byte request should always pass
	auto result = limiter->UntilNReady(0);
	REQUIRE(result == RateLimitResult::Allowed);
}

TEST_CASE("Burst limit - Check method returns InsufficientCapacity for oversized request", "[burst]") {
	auto clock = CreateMockClock();
	auto quota = Quota::PerSecond(1000).AllowBurst(100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// Check should return InsufficientCapacity for request exceeding burst
	auto result = limiter->Check(200);
	REQUIRE(result == RateLimitResult::InsufficientCapacity);
}

TEST_CASE("Burst limit - TryAcquireImmediate returns max wait for oversized request", "[burst]") {
	auto clock = CreateMockClock();
	auto quota = Quota::PerSecond(1000).AllowBurst(100);
	auto limiter = RateLimiter::Direct(quota, clock);

	// TryAcquireImmediate should return max duration for request exceeding burst
	auto wait_info = limiter->TryAcquireImmediate(200);
	REQUIRE(wait_info.has_value());
	REQUIRE(wait_info->wait_duration == Duration::max());
}

TEST_CASE("Burst limit - ThrottleLayer Read fails for request exceeding burst", "[burst][throttle]") {
	auto clock = CreateMockClock();
	// 1000 bytes/sec, 100 byte burst
	ThrottleLayer throttle(1000, 100, clock);

	// Read request exceeding burst should fail
	auto result = throttle.Read("/test/file", 0, 200);
	REQUIRE_FALSE(result.success);
	REQUIRE(result.error == ThrottleError::RequestExceedsBurst);
}

TEST_CASE("Burst limit - ThrottleLayer Read passes for request within burst", "[burst][throttle]") {
	auto clock = CreateMockClock();
	// 1000 bytes/sec, 100 byte burst
	ThrottleLayer throttle(1000, 100, clock);

	// Read request within burst should pass
	auto result = throttle.Read("/test/file", 0, 50);
	REQUIRE(result.success);
	REQUIRE(result.bytes_read == 50);
}

TEST_CASE("Burst limit - ThrottleLayer Write fails for request exceeding burst", "[burst][throttle]") {
	auto clock = CreateMockClock();
	// 1000 bytes/sec, 100 byte burst
	ThrottleLayer throttle(1000, 100, clock);

	// Write request exceeding burst should fail
	auto result = throttle.Write("/test/file", 200);
	REQUIRE_FALSE(result.success);
	REQUIRE(result.error == ThrottleError::RequestExceedsBurst);
}

TEST_CASE("Burst limit - ThrottleLayer Write passes for request within burst", "[burst][throttle]") {
	auto clock = CreateMockClock();
	// 1000 bytes/sec, 100 byte burst
	ThrottleLayer throttle(1000, 100, clock);

	// Write request within burst should pass
	auto result = throttle.Write("/test/file", 50);
	REQUIRE(result.success);
	REQUIRE(result.bytes_written == 50);
}

TEST_CASE("Burst limit - ThrottleLayer zero byte operations always pass", "[burst][throttle]") {
	auto clock = CreateMockClock();
	ThrottleLayer throttle(1000, 100, clock);

	// Zero-byte read should pass
	auto read_result = throttle.Read("/test/file", 0, 0);
	REQUIRE(read_result.success);
	REQUIRE(read_result.bytes_read == 0);

	// Zero-byte write should pass
	auto write_result = throttle.Write("/test/file", 0);
	REQUIRE(write_result.success);
	REQUIRE(write_result.bytes_written == 0);
}

TEST_CASE("Burst limit - ThrottleLayer negative byte read fails", "[burst][throttle]") {
	auto clock = CreateMockClock();
	ThrottleLayer throttle(1000, 100, clock);

	// Negative byte read should fail
	auto result = throttle.Read("/test/file", 0, -1);
	REQUIRE_FALSE(result.success);
	REQUIRE(result.error == ThrottleError::RequestExceedsBurst);
}

TEST_CASE("Burst limit - ThrottleLayer negative byte write fails", "[burst][throttle]") {
	auto clock = CreateMockClock();
	ThrottleLayer throttle(1000, 100, clock);

	// Negative byte write should fail
	auto result = throttle.Write("/test/file", -1);
	REQUIRE_FALSE(result.success);
	REQUIRE(result.error == ThrottleError::RequestExceedsBurst);
}

TEST_CASE("Burst limit - Quota creation with zero bandwidth throws", "[burst][quota]") {
	REQUIRE_THROWS_AS(Quota::PerSecond(0), InvalidInputException);
}

TEST_CASE("Burst limit - Quota AllowBurst with zero burst throws", "[burst][quota]") {
	auto quota = Quota::PerSecond(1000);
	REQUIRE_THROWS_AS(quota.AllowBurst(0), InvalidInputException);
}

TEST_CASE("Burst limit - ThrottleLayer creation with zero bandwidth throws", "[burst][throttle]") {
	auto clock = CreateMockClock();
	REQUIRE_THROWS_AS(ThrottleLayer(0, 100, clock), InvalidInputException);
}

TEST_CASE("Burst limit - ThrottleLayer creation with zero burst throws", "[burst][throttle]") {
	auto clock = CreateMockClock();
	REQUIRE_THROWS_AS(ThrottleLayer(1000, 0, clock), InvalidInputException);
}
