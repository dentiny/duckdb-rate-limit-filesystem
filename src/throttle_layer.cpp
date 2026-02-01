#include "throttle_layer.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

ThrottleLayer::ThrottleLayer(uint32_t bandwidth_p, uint32_t burst_p, shared_ptr<BaseClock> clock_p)
    : bandwidth(bandwidth_p), burst(burst_p), api_rate(0), api_limiter(nullptr) {
	if (bandwidth_p == 0) {
		throw InvalidInputException("bandwidth must be greater than 0");
	}
	if (burst_p == 0) {
		throw InvalidInputException("burst must be greater than 0");
	}

	auto quota = Quota::PerSecond(bandwidth_p).AllowBurst(burst_p);
	bandwidth_limiter = RateLimiter::Direct(quota, clock_p);
}

ThrottleLayer::ThrottleLayer(uint32_t bandwidth_p, uint32_t burst_p, uint32_t api_rate_p, shared_ptr<BaseClock> clock_p)
    : bandwidth(bandwidth_p), burst(burst_p), api_rate(api_rate_p) {
	if (bandwidth_p == 0) {
		throw InvalidInputException("bandwidth must be greater than 0");
	}
	if (burst_p == 0) {
		throw InvalidInputException("burst must be greater than 0");
	}
	if (api_rate_p == 0) {
		throw InvalidInputException("api_rate must be greater than 0");
	}

	// Create bandwidth rate limiter
	auto bw_quota = Quota::PerSecond(bandwidth_p).AllowBurst(burst_p);
	bandwidth_limiter = RateLimiter::Direct(bw_quota, clock_p);

	// Create API rate limiter
	// For API rate limiting, burst = rate (each call is 1 unit, no burst concept)
	auto api_quota = Quota::PerSecond(api_rate_p).AllowBurst(api_rate_p);
	api_limiter = RateLimiter::Direct(api_quota, clock_p);
}

ThrottleLayer::ThrottleLayer(const ThrottleLayer &other)
    : bandwidth(other.bandwidth), burst(other.burst), api_rate(other.api_rate),
      bandwidth_limiter(other.bandwidth_limiter), api_limiter(other.api_limiter) {
}

ThrottleLayer &ThrottleLayer::operator=(const ThrottleLayer &other) {
	if (this != &other) {
		bandwidth = other.bandwidth;
		burst = other.burst;
		api_rate = other.api_rate;
		bandwidth_limiter = other.bandwidth_limiter;
		api_limiter = other.api_limiter;
	}
	return *this;
}

ThrottleLayer::ThrottleLayer(ThrottleLayer &&other) noexcept
    : bandwidth(other.bandwidth), burst(other.burst), api_rate(other.api_rate),
      bandwidth_limiter(std::move(other.bandwidth_limiter)), api_limiter(std::move(other.api_limiter)) {
	other.bandwidth = 0;
	other.burst = 0;
	other.api_rate = 0;
}

ThrottleLayer &ThrottleLayer::operator=(ThrottleLayer &&other) noexcept {
	if (this != &other) {
		bandwidth = other.bandwidth;
		burst = other.burst;
		api_rate = other.api_rate;
		bandwidth_limiter = std::move(other.bandwidth_limiter);
		api_limiter = std::move(other.api_limiter);
		other.bandwidth = 0;
		other.burst = 0;
		other.api_rate = 0;
	}
	return *this;
}

ThrottleLayer::~ThrottleLayer() = default;

ReadResult ThrottleLayer::Read(const string &path, int start_offset, int bytes_to_read) {
	// Validate input
	if (bytes_to_read < 0) {
		return ReadResult::Error(ThrottleError::RequestExceedsBurst, "bytes_to_read cannot be negative");
	}

	// Zero-byte reads are always allowed
	if (bytes_to_read == 0) {
		return ReadResult::Success(0);
	}

	// Check if the request exceeds uint32_t max
	if (static_cast<uint64_t>(bytes_to_read) > NumericLimits<uint32_t>::Maximum()) {
		stringstream oss;
		oss << "request size (" << bytes_to_read << " bytes) exceeds throttle quota capacity";
		return ReadResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
	}

	uint32_t request_size = static_cast<uint32_t>(bytes_to_read);

	// Check if request exceeds burst capacity
	if (request_size > burst) {
		stringstream oss;
		oss << "burst size (" << burst << " bytes) is smaller than the request size (" << request_size << " bytes)";
		return ReadResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
	}

	// Wait for API rate limit (if enabled)
	if (api_limiter) {
		auto api_result = api_limiter->UntilNReady(1);
		if (api_result == RateLimitResult::InsufficientCapacity) {
			return ReadResult::Error(ThrottleError::RateLimited, "API rate limit exceeded");
		}
	}

	// Wait until bandwidth is available
	auto result = bandwidth_limiter->UntilNReady(request_size);
	if (result == RateLimitResult::InsufficientCapacity) {
		stringstream oss;
		oss << "burst size (" << burst << " bytes) is smaller than the request size (" << request_size << " bytes)";
		return ReadResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
	}

	// In a real implementation, we would perform the actual read here:
	// return inner_operator->Read(path, start_offset, bytes_to_read);
	//
	// For this standalone implementation, we simulate success and
	// return the requested bytes as "read"
	return ReadResult::Success(static_cast<size_t>(bytes_to_read));
}

WriteResult ThrottleLayer::Write(const string &path, int bytes_to_write) {
	// Validate input
	if (bytes_to_write < 0) {
		return WriteResult::Error(ThrottleError::RequestExceedsBurst, "bytes_to_write cannot be negative");
	}

	// Zero-byte writes are always allowed
	if (bytes_to_write == 0) {
		return WriteResult::Success(0);
	}

	// Check if the request exceeds uint32_t max
	if (static_cast<uint64_t>(bytes_to_write) > NumericLimits<uint32_t>::Maximum()) {
		stringstream oss;
		oss << "request size (" << bytes_to_write << " bytes) exceeds throttle quota capacity";
		return WriteResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
	}

	uint32_t request_size = static_cast<uint32_t>(bytes_to_write);

	// Check if request exceeds burst capacity
	if (request_size > burst) {
		stringstream oss;
		oss << "burst size (" << burst << " bytes) is smaller than the request size (" << request_size << " bytes)";
		return WriteResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
	}

	// Wait for API rate limit (if enabled)
	if (api_limiter) {
		auto api_result = api_limiter->UntilNReady(1);
		if (api_result == RateLimitResult::InsufficientCapacity) {
			return WriteResult::Error(ThrottleError::RateLimited, "API rate limit exceeded");
		}
	}

	// Wait until bandwidth is available
	auto result = bandwidth_limiter->UntilNReady(request_size);
	if (result == RateLimitResult::InsufficientCapacity) {
		stringstream oss;
		oss << "burst size (" << burst << " bytes) is smaller than the request size (" << request_size << " bytes)";
		return WriteResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
	}

	// In a real implementation, we would perform the actual write here:
	// return inner_operator->Write(path, data);
	//
	// For this standalone implementation, we simulate success and
	// return the requested bytes as "written"
	return WriteResult::Success(static_cast<size_t>(bytes_to_write));
}

uint32_t ThrottleLayer::GetBandwidth() const {
	return bandwidth;
}

uint32_t ThrottleLayer::GetBurst() const {
	return burst;
}

uint32_t ThrottleLayer::GetApiRate() const {
	return api_rate;
}

bool ThrottleLayer::HasApiRateLimiting() const {
	return api_limiter != nullptr;
}

SharedRateLimiter ThrottleLayer::GetBandwidthRateLimiter() const {
	return bandwidth_limiter;
}

SharedRateLimiter ThrottleLayer::GetApiRateLimiter() const {
	return api_limiter;
}

} // namespace duckdb
