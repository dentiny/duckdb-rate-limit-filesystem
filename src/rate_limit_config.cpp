#include "rate_limit_config.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "no_destructor.hpp"

namespace duckdb {

namespace {

// Valid filesystem operations that can be rate limited
const NoDestructor<vector<string>> VALID_OPERATIONS {{"open", "stat", "read", "write", "list", "delete"}};

bool IsValidOperation(const string &op) {
	for (const auto &valid_op : *VALID_OPERATIONS) {
		if (op == valid_op) {
			return true;
		}
	}
	return false;
}

} // namespace

string NormalizeOperation(const string &operation) {
	auto op_lower = StringUtil::Lower(operation);
	if (!IsValidOperation(op_lower)) {
		throw InvalidInputException(
		    "Invalid operation '%s'. Valid operations are: open, stat, read, write, list, delete", operation);
	}
	return op_lower;
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
	auto op = NormalizeOperation(operation);
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);

	auto it = configs.find(op);
	if (it == configs.end()) {
		if (value == 0) {
			// No config exists and quota is 0, nothing to do
			return;
		}
		// Create new config
		OperationConfig config;
		config.operation = op;
		config.quota = value;
		config.mode = mode;
		config.burst = 0;
		configs[op] = config;
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
	auto &config = configs[op];
	UpdateRateLimiter(config);
}

void RateLimitConfig::SetBurst(const string &operation, idx_t value) {
	auto op = NormalizeOperation(operation);
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);

	auto it = configs.find(op);
	if (it == configs.end()) {
		if (value == 0) {
			// No config exists and burst is 0, nothing to do
			return;
		}
		// Create new config with only burst
		OperationConfig config;
		config.operation = op;
		config.quota = 0;
		config.mode = RateLimitMode::BLOCKING;
		config.burst = value;
		configs[op] = config;
	} else {
		it->second.burst = value;
		// If both quota and burst are 0, remove the config
		if (it->second.quota == 0 && it->second.burst == 0) {
			configs.erase(it);
			return;
		}
	}

	// Update the rate limiter
	auto &config = configs[op];
	UpdateRateLimiter(config);
}

const OperationConfig *RateLimitConfig::GetConfig(const string &operation) const {
	auto op = StringUtil::Lower(operation);
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(op);
	if (it == configs.end()) {
		return nullptr;
	}
	return &it->second;
}

SharedRateLimiter RateLimitConfig::GetOrCreateRateLimiter(const string &operation) {
	auto op = StringUtil::Lower(operation);
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(op);
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

void RateLimitConfig::ClearConfig(const string &operation) {
	auto op = StringUtil::Lower(operation);
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	configs.erase(op);
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

void RateLimitConfig::UpdateRateLimiter(OperationConfig &config) {
	// Need at least one of quota or burst to create a rate limiter
	if (config.quota == 0 && config.burst == 0) {
		config.rate_limiter = nullptr;
		return;
	}

	// Create new rate limiter with current settings
	config.rate_limiter = CreateRateLimiter(config.quota, config.burst);
}

} // namespace duckdb
