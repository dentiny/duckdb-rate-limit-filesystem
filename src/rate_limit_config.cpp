#include "rate_limit_config.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"

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

void RateLimitConfig::SetQuota(const string &filesystem_name, FileSystemOperation operation, idx_t value,
                               RateLimitMode mode) {
	ConfigKey key {filesystem_name, operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		if (value == 0) {
			// No config exists and quota is 0, nothing to do
			return;
		}
		// Create new config
		OperationConfig config;
		config.filesystem_name = filesystem_name;
		config.operation = operation;
		config.quota = value;
		config.mode = mode;
		config.burst = 0;
		it = configs.emplace(key, config).first;
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

void RateLimitConfig::SetBurst(const string &filesystem_name, FileSystemOperation operation, idx_t value) {
	// Burst only makes sense for byte-based operations (READ/WRITE)
	if (operation != FileSystemOperation::READ && operation != FileSystemOperation::WRITE) {
		throw InvalidInputException("Burst limit can only be set for READ or WRITE operations, not '%s'",
		                            FileSystemOperationToString(operation));
	}

	ConfigKey key {filesystem_name, operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		if (value == 0) {
			// No config exists and burst is 0, nothing to do
			return;
		}
		// Create new config with only burst
		OperationConfig config;
		config.filesystem_name = filesystem_name;
		config.operation = operation;
		config.quota = 0;
		config.mode = RateLimitMode::BLOCKING;
		config.burst = value;
		it = configs.emplace(key, config).first;
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

const OperationConfig *RateLimitConfig::GetConfig(const string &filesystem_name, FileSystemOperation operation) const {
	ConfigKey key {filesystem_name, operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		return nullptr;
	}
	return &it->second;
}

SharedRateLimiter RateLimitConfig::GetOrCreateRateLimiter(const string &filesystem_name,
                                                          FileSystemOperation operation) {
	ConfigKey key {filesystem_name, operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
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

vector<OperationConfig> RateLimitConfig::GetConfigsForFilesystem(const string &filesystem_name) const {
	vector<OperationConfig> result;

	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	for (const auto &pair : configs) {
		if (pair.first.filesystem_name == filesystem_name) {
			result.push_back(pair.second);
		}
	}
	return result;
}

void RateLimitConfig::ClearConfig(const string &filesystem_name, FileSystemOperation operation) {
	ConfigKey key {filesystem_name, operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	configs.erase(key);
}

void RateLimitConfig::ClearFilesystem(const string &filesystem_name) {
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	for (auto it = configs.begin(); it != configs.end();) {
		if (it->first.filesystem_name == filesystem_name) {
			it = configs.erase(it);
		} else {
			++it;
		}
	}
}

void RateLimitConfig::ClearAll() {
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	configs.clear();
}

shared_ptr<RateLimitConfig> RateLimitConfig::GetOrCreate(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	auto config = cache.GetOrCreate<RateLimitConfig>(CACHE_KEY);

	// Set the database instance for logging (if not already set)
	{
		concurrency::lock_guard<concurrency::mutex> guard(config->config_lock);
		if (config->db_instance.expired()) {
			auto &db = DatabaseInstance::GetDatabase(context);
			config->db_instance = db.shared_from_this();
		}
	}

	return config;
}

shared_ptr<RateLimitConfig> RateLimitConfig::Get(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.Get<RateLimitConfig>(CACHE_KEY);
}

shared_ptr<DatabaseInstance> RateLimitConfig::GetDatabaseInstance() const {
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto db = db_instance.lock();
	if (!db) {
		throw InternalException("Database instance is no longer available for rate limit logging");
	}
	return db;
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
