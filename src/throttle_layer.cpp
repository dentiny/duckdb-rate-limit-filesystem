#include "throttle_layer.hpp"

#include <algorithm>
#include <limits>
#include <sstream>

namespace duckdb {

ThrottleLayer::ThrottleLayer(uint32_t bandwidth, uint32_t burst,
                             std::shared_ptr<BaseClock> clock)
    : bandwidth_(bandwidth), burst_(burst), api_rate_(0), api_limiter_(nullptr) {
  if (bandwidth == 0) {
    throw std::invalid_argument("bandwidth must be greater than 0");
  }
  if (burst == 0) {
    throw std::invalid_argument("burst must be greater than 0");
  }

  auto quota = Quota::PerSecond(bandwidth).AllowBurst(burst);
  bandwidth_limiter_ = RateLimiter::Direct(quota, clock);
}

ThrottleLayer::ThrottleLayer(uint32_t bandwidth, uint32_t burst,
                             uint32_t api_rate, std::shared_ptr<BaseClock> clock)
    : bandwidth_(bandwidth), burst_(burst), api_rate_(api_rate) {
  if (bandwidth == 0) {
    throw std::invalid_argument("bandwidth must be greater than 0");
  }
  if (burst == 0) {
    throw std::invalid_argument("burst must be greater than 0");
  }
  if (api_rate == 0) {
    throw std::invalid_argument("api_rate must be greater than 0");
  }

  // Create bandwidth rate limiter
  auto bw_quota = Quota::PerSecond(bandwidth).AllowBurst(burst);
  bandwidth_limiter_ = RateLimiter::Direct(bw_quota, clock);

  // Create API rate limiter
  // For API rate limiting, burst = rate (each call is 1 unit, no burst concept)
  auto api_quota = Quota::PerSecond(api_rate).AllowBurst(api_rate);
  api_limiter_ = RateLimiter::Direct(api_quota, clock);
}

ThrottleLayer::ThrottleLayer(const ThrottleLayer& other)
    : bandwidth_(other.bandwidth_),
      burst_(other.burst_),
      api_rate_(other.api_rate_),
      bandwidth_limiter_(other.bandwidth_limiter_),
      api_limiter_(other.api_limiter_) {}

ThrottleLayer& ThrottleLayer::operator=(const ThrottleLayer& other) {
  if (this != &other) {
    bandwidth_ = other.bandwidth_;
    burst_ = other.burst_;
    api_rate_ = other.api_rate_;
    bandwidth_limiter_ = other.bandwidth_limiter_;
    api_limiter_ = other.api_limiter_;
  }
  return *this;
}

ThrottleLayer::ThrottleLayer(ThrottleLayer&& other) noexcept
    : bandwidth_(other.bandwidth_),
      burst_(other.burst_),
      api_rate_(other.api_rate_),
      bandwidth_limiter_(std::move(other.bandwidth_limiter_)),
      api_limiter_(std::move(other.api_limiter_)) {
  other.bandwidth_ = 0;
  other.burst_ = 0;
  other.api_rate_ = 0;
}

ThrottleLayer& ThrottleLayer::operator=(ThrottleLayer&& other) noexcept {
  if (this != &other) {
    bandwidth_ = other.bandwidth_;
    burst_ = other.burst_;
    api_rate_ = other.api_rate_;
    bandwidth_limiter_ = std::move(other.bandwidth_limiter_);
    api_limiter_ = std::move(other.api_limiter_);
    other.bandwidth_ = 0;
    other.burst_ = 0;
    other.api_rate_ = 0;
  }
  return *this;
}

ThrottleLayer::~ThrottleLayer() = default;

ReadResult ThrottleLayer::Read(const std::string& path, int start_offset,
                               int bytes_to_read) {
  // Validate input
  if (bytes_to_read < 0) {
    return ReadResult::Error(ThrottleError::RequestExceedsBurst,
                             "bytes_to_read cannot be negative");
  }

  // Zero-byte reads are always allowed
  if (bytes_to_read == 0) {
    return ReadResult::Success(0);
  }

  // Check if the request exceeds uint32_t max
  if (static_cast<uint64_t>(bytes_to_read) >
      std::numeric_limits<uint32_t>::max()) {
    std::ostringstream oss;
    oss << "request size (" << bytes_to_read
        << " bytes) exceeds throttle quota capacity";
    return ReadResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
  }

  uint32_t request_size = static_cast<uint32_t>(bytes_to_read);

  // Check if request exceeds burst capacity
  if (request_size > burst_) {
    std::ostringstream oss;
    oss << "burst size (" << burst_
        << " bytes) is smaller than the request size (" << request_size
        << " bytes)";
    return ReadResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
  }

  // Wait for API rate limit (if enabled)
  if (api_limiter_) {
    auto api_result = api_limiter_->UntilNReady(1);
    if (api_result == RateLimitResult::InsufficientCapacity) {
      return ReadResult::Error(ThrottleError::RateLimited,
                               "API rate limit exceeded");
    }
  }

  // Wait until bandwidth is available
  auto result = bandwidth_limiter_->UntilNReady(request_size);
  if (result == RateLimitResult::InsufficientCapacity) {
    std::ostringstream oss;
    oss << "burst size (" << burst_
        << " bytes) is smaller than the request size (" << request_size
        << " bytes)";
    return ReadResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
  }

  // In a real implementation, we would perform the actual read here:
  // return inner_operator_->Read(path, start_offset, bytes_to_read);
  //
  // For this standalone implementation, we simulate success and
  // return the requested bytes as "read"
  return ReadResult::Success(static_cast<size_t>(bytes_to_read));
}

WriteResult ThrottleLayer::Write(const std::string& path, int bytes_to_write) {
  // Validate input
  if (bytes_to_write < 0) {
    return WriteResult::Error(ThrottleError::RequestExceedsBurst,
                              "bytes_to_write cannot be negative");
  }

  // Zero-byte writes are always allowed
  if (bytes_to_write == 0) {
    return WriteResult::Success(0);
  }

  // Check if the request exceeds uint32_t max
  if (static_cast<uint64_t>(bytes_to_write) >
      std::numeric_limits<uint32_t>::max()) {
    std::ostringstream oss;
    oss << "request size (" << bytes_to_write
        << " bytes) exceeds throttle quota capacity";
    return WriteResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
  }

  uint32_t request_size = static_cast<uint32_t>(bytes_to_write);

  // Check if request exceeds burst capacity
  if (request_size > burst_) {
    std::ostringstream oss;
    oss << "burst size (" << burst_
        << " bytes) is smaller than the request size (" << request_size
        << " bytes)";
    return WriteResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
  }

  // Wait for API rate limit (if enabled)
  if (api_limiter_) {
    auto api_result = api_limiter_->UntilNReady(1);
    if (api_result == RateLimitResult::InsufficientCapacity) {
      return WriteResult::Error(ThrottleError::RateLimited,
                                "API rate limit exceeded");
    }
  }

  // Wait until bandwidth is available
  auto result = bandwidth_limiter_->UntilNReady(request_size);
  if (result == RateLimitResult::InsufficientCapacity) {
    std::ostringstream oss;
    oss << "burst size (" << burst_
        << " bytes) is smaller than the request size (" << request_size
        << " bytes)";
    return WriteResult::Error(ThrottleError::RequestExceedsBurst, oss.str());
  }

  // In a real implementation, we would perform the actual write here:
  // return inner_operator_->Write(path, data);
  //
  // For this standalone implementation, we simulate success and
  // return the requested bytes as "written"
  return WriteResult::Success(static_cast<size_t>(bytes_to_write));
}

uint32_t ThrottleLayer::GetBandwidth() const {
  return bandwidth_;
}

uint32_t ThrottleLayer::GetBurst() const {
  return burst_;
}

uint32_t ThrottleLayer::GetApiRate() const {
  return api_rate_;
}

bool ThrottleLayer::HasApiRateLimiting() const {
  return api_limiter_ != nullptr;
}

SharedRateLimiter ThrottleLayer::GetBandwidthRateLimiter() const {
  return bandwidth_limiter_;
}

SharedRateLimiter ThrottleLayer::GetApiRateLimiter() const {
  return api_limiter_;
}

}  // namespace duckdb
