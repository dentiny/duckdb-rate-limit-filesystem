#include "rate_limit_config.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

RateLimitConfig::RateLimitConfig() {
}

RateLimitConfig::~RateLimitConfig() {
}

string RateLimitConfig::GetObjectType() {
	return OBJECT_TYPE;
}

string RateLimitConfig::ObjectType() {
	return OBJECT_TYPE;
}

void RateLimitConfig::SetQuota(FileSystemOperation operation, idx_t value, RateLimitMode mode) {
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);

	auto it = configs.find(operation);
	if (it == configs.end()) {
		if (value == 0) {
			// No config exists and quota is 0, nothing to do
			return;
		}
		// Create new config
		OperationConfig config;
		config.operation = operation;
		config.quota = value;
		config.mode = mode;
		config.burst = 0;
		it = configs.emplace(operation, config).first;
	} else {
		it->second.quota = value;
		it->second.mode = mode;
		// If both quota and burst are 0, remove the config
		if (it->second.quota == 0 && it->second.burst == 0) {
			configs.erase(it);
			return;
		}
	}

	UpdateRateLimiter(it->second);
}

void RateLimitConfig::SetBurst(FileSystemOperation operation, idx_t value) {
	// Burst only makes sense for byte-based operations (READ/WRITE)
	if (operation != FileSystemOperation::READ && operation != FileSystemOperation::WRITE) {
		throw InvalidInputException("Burst limit can only be set for READ or WRITE operations, not '%s'",
		                            FileSystemOperationToString(operation));
	}

	concurrency::lock_guard<concurrency::mutex> guard(config_lock);

	auto it = configs.find(operation);
	if (it == configs.end()) {
		if (value == 0) {
			// No config exists and burst is 0, nothing to do
			return;
		}
		// Create new config with only burst
		OperationConfig config;
		config.operation = operation;
		config.quota = 0;
		config.mode = RateLimitMode::BLOCKING;
		config.burst = value;
		it = configs.emplace(operation, config).first;
	} else {
		it->second.burst = value;
		// If both quota and burst are 0, remove the config
		if (it->second.quota == 0 && it->second.burst == 0) {
			configs.erase(it);
			return;
		}
	}

	UpdateRateLimiter(it->second);
}

const OperationConfig *RateLimitConfig::GetConfig(FileSystemOperation operation) const {
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(operation);
	if (it == configs.end()) {
		return nullptr;
	}
	return &it->second;
}

SharedRateLimiter RateLimitConfig::GetOrCreateRateLimiter(FileSystemOperation operation) {
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(operation);
	if (it == configs.end()) {
		return nullptr;
	}

	if (!it->second.rate_limiter) {
		UpdateRateLimiter(it->second);
	}
	return it->second.rate_limiter;
}

vector<OperationConfig> RateLimitConfig::GetAllConfigs() const {
	vector<OperationConfig> result;
	result.reserve(configs.size());

	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	for (const auto &pair : configs) {
		result.push_back(pair.second);
	}
	return result;
}

void RateLimitConfig::ClearConfig(FileSystemOperation operation) {
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	configs.erase(operation);
}

void RateLimitConfig::ClearAll() {
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	configs.clear();
}

shared_ptr<RateLimitConfig> RateLimitConfig::GetOrCreate(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<RateLimitConfig>(CACHE_KEY);
}

shared_ptr<RateLimitConfig> RateLimitConfig::Get(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.Get<RateLimitConfig>(CACHE_KEY);
}

void RateLimitConfig::SetClock(shared_ptr<BaseClock> clock_p) {
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	clock = std::move(clock_p);

	// Update all existing rate limiters to use the new clock
	for (auto &pair : configs) {
		UpdateRateLimiter(pair.second);
	}
}

void RateLimitConfig::UpdateRateLimiter(OperationConfig &config) {
	// Config should have been removed from the map if both quota and burst are 0
	D_ASSERT(config.quota > 0 || config.burst > 0);

	// Create new rate limiter with current settings, using the configured clock
	config.rate_limiter = CreateRateLimiter(config.quota, config.burst, clock);
}

} // namespace duckdb
