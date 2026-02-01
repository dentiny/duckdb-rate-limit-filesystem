#include "rate_limit_config.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

RateLimitMode ParseRateLimitMode(const string &mode_str) {
	if (mode_str == "blocking" || mode_str == "block") {
		return RateLimitMode::BLOCKING;
	} else if (mode_str == "non_blocking" || mode_str == "non-blocking" || mode_str == "nonblocking") {
		return RateLimitMode::NON_BLOCKING;
	}
	throw InvalidInputException("Invalid rate limit mode '%s'. Use 'blocking' or 'non_blocking'", mode_str);
}

string RateLimitModeToString(RateLimitMode mode) {
	switch (mode) {
	case RateLimitMode::BLOCKING:
		return "blocking";
	case RateLimitMode::NON_BLOCKING:
		return "non_blocking";
	default:
		return "unknown";
	}
}

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

void RateLimitConfig::SetQuota(const string &operation, idx_t value, RateLimitMode mode) {
	lock_guard<mutex> guard(config_lock);

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
		configs[operation] = config;
	} else {
		it->second.quota = value;
		it->second.mode = mode;
		// If both quota and burst are 0, remove the config
		if (it->second.quota == 0 && it->second.burst == 0) {
			configs.erase(it);
			return;
		}
	}

	// Update the rate limiter
	auto &config = configs[operation];
	UpdateRateLimiter(config);
}

void RateLimitConfig::SetBurst(const string &operation, idx_t value) {
	lock_guard<mutex> guard(config_lock);

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
		configs[operation] = config;
	} else {
		it->second.burst = value;
		// If both quota and burst are 0, remove the config
		if (it->second.quota == 0 && it->second.burst == 0) {
			configs.erase(it);
			return;
		}
	}

	// Update the rate limiter
	auto &config = configs[operation];
	UpdateRateLimiter(config);
}

const OperationConfig *RateLimitConfig::GetConfig(const string &operation) const {
	lock_guard<mutex> guard(config_lock);
	auto it = configs.find(operation);
	if (it == configs.end()) {
		return nullptr;
	}
	return &it->second;
}

SharedRateLimiter RateLimitConfig::GetOrCreateRateLimiter(const string &operation) {
	lock_guard<mutex> guard(config_lock);
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
	lock_guard<mutex> guard(config_lock);
	vector<OperationConfig> result;
	result.reserve(configs.size());
	for (const auto &pair : configs) {
		result.push_back(pair.second);
	}
	return result;
}

void RateLimitConfig::ClearConfig(const string &operation) {
	lock_guard<mutex> guard(config_lock);
	configs.erase(operation);
}

void RateLimitConfig::ClearAll() {
	lock_guard<mutex> guard(config_lock);
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

void RateLimitConfig::UpdateRateLimiter(OperationConfig &config) {
	// Need at least one of quota or burst to create a rate limiter
	if (config.quota == 0 && config.burst == 0) {
		config.rate_limiter = nullptr;
		return;
	}

	// Create new rate limiter with current settings
	// Note: The Quota class handles the case where one of bandwidth/burst is 0
	config.rate_limiter = CreateRateLimiter(config.quota, config.burst);
}

} // namespace duckdb
