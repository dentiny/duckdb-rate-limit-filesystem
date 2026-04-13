#include "rate_limit_config.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/path.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

RateLimitConfig::RateLimitConfig() = default;

RateLimitConfig::~RateLimitConfig() = default;

string RateLimitConfig::GetObjectType() {
	return OBJECT_TYPE;
}

string RateLimitConfig::ObjectType() {
	return OBJECT_TYPE;
}

void RateLimitConfig::SetQuota(const string &filesystem_name, FileSystemOperation operation, idx_t value,
                               RateLimitMode mode) {
	ConfigKey key {filesystem_name, "", operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		if (value == 0) {
			return;
		}
		OperationConfig config;
		config.filesystem_name = filesystem_name;
		config.bucket = "";
		config.operation = operation;
		config.quota = value;
		config.mode = mode;
		it = configs.emplace(key, config).first;
	} else {
		it->second.quota = value;
		it->second.mode = mode;
		if (it->second.IsEmpty()) {
			configs.erase(it);
			return;
		}
	}

	UpdateRateLimiter(it->second);
}

void RateLimitConfig::SetBurst(const string &filesystem_name, FileSystemOperation operation, idx_t value) {
	if (operation != FileSystemOperation::READ && operation != FileSystemOperation::WRITE) {
		throw InvalidInputException("Burst limit can only be set for READ or WRITE operations, not '%s'",
		                            FileSystemOperationToString(operation));
	}

	ConfigKey key {filesystem_name, "", operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		if (value == 0) {
			return;
		}
		OperationConfig config;
		config.filesystem_name = filesystem_name;
		config.bucket = "";
		config.operation = operation;
		config.quota = 0;
		config.mode = RateLimitMode::BLOCKING;
		config.burst = value;
		it = configs.emplace(key, config).first;
	} else {
		it->second.burst = value;
		if (it->second.IsEmpty()) {
			configs.erase(it);
			return;
		}
	}

	UpdateRateLimiter(it->second);
}

void RateLimitConfig::SetMaxRequests(const string &filesystem_name, FileSystemOperation operation, int64_t value) {
	if (value < CountingSemaphore::UNLIMITED) {
		throw InvalidInputException("Max requests value must be -1 (unlimited) or a positive integer, got %lld", value);
	}
	if (value == 0) {
		throw InvalidInputException("Max requests value cannot be 0 (would block all requests)");
	}

	ConfigKey key {filesystem_name, "", operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		if (value == CountingSemaphore::UNLIMITED) {
			return;
		}
		OperationConfig config;
		config.filesystem_name = filesystem_name;
		config.bucket = "";
		config.operation = operation;
		config.max_requests = value;
		config.semaphore = make_shared_ptr<CountingSemaphore>(value);
		it = configs.emplace(key, config).first;
	} else {
		it->second.max_requests = value;
		if (it->second.IsEmpty()) {
			configs.erase(it);
			return;
		}
		if (value == CountingSemaphore::UNLIMITED) {
			it->second.semaphore = nullptr;
		} else if (it->second.semaphore) {
			it->second.semaphore->SetMax(value);
		} else {
			it->second.semaphore = make_shared_ptr<CountingSemaphore>(value);
		}
	}
}

const OperationConfig *RateLimitConfig::GetConfig(const string &filesystem_name, FileSystemOperation operation) const {
	ConfigKey key {filesystem_name, "", operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		return nullptr;
	}
	return &it->second;
}

SharedRateLimiter RateLimitConfig::GetOrCreateRateLimiter(const string &filesystem_name,
                                                          FileSystemOperation operation) {
	ConfigKey key {filesystem_name, "", operation};
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

shared_ptr<CountingSemaphore> RateLimitConfig::GetOrCreateSemaphore(const string &filesystem_name,
                                                                    FileSystemOperation operation) {
	ConfigKey key {filesystem_name, "", operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		return nullptr;
	}

	if (it->second.max_requests == CountingSemaphore::UNLIMITED) {
		return nullptr;
	}

	if (!it->second.semaphore) {
		it->second.semaphore = make_shared_ptr<CountingSemaphore>(it->second.max_requests);
	}
	return it->second.semaphore;
}

RateLimitConfig::RateLimitSnapshot RateLimitConfig::GetRateLimitSnapshot(const string &filesystem_name,
                                                                         FileSystemOperation operation) {
	RateLimitSnapshot snapshot;
	ConfigKey key {filesystem_name, "", operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		return snapshot;
	}

	auto &op_config = it->second;
	snapshot.mode = op_config.mode;

	// Ensure rate limiter exists if quota/burst are set.
	if (op_config.quota > 0 || op_config.burst > 0) {
		if (!op_config.rate_limiter) {
			UpdateRateLimiter(op_config);
		}
		snapshot.rate_limiter = op_config.rate_limiter;
	}

	if (op_config.max_requests != CountingSemaphore::UNLIMITED) {
		D_ASSERT(op_config.semaphore);
		snapshot.semaphore = op_config.semaphore;
	}

	return snapshot;
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
	ConfigKey key {filesystem_name, "", operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	configs.erase(key);
}

void RateLimitConfig::SetQuotaBucket(const string &filesystem_name, const string &bucket, FileSystemOperation operation,
                                     idx_t value, RateLimitMode mode) {
	ConfigKey key {filesystem_name, bucket, operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		if (value == 0) {
			return;
		}
		OperationConfig config;
		config.filesystem_name = filesystem_name;
		config.bucket = bucket;
		config.operation = operation;
		config.quota = value;
		config.mode = mode;
		it = configs.emplace(key, config).first;
	} else {
		it->second.quota = value;
		it->second.mode = mode;
		if (it->second.IsEmpty()) {
			configs.erase(it);
			return;
		}
	}

	UpdateRateLimiter(it->second);
}

void RateLimitConfig::SetBurstBucket(const string &filesystem_name, const string &bucket, FileSystemOperation operation,
                                     idx_t value) {
	if (operation != FileSystemOperation::READ && operation != FileSystemOperation::WRITE) {
		throw InvalidInputException("Burst limit can only be set for READ or WRITE operations, not '%s'",
		                            FileSystemOperationToString(operation));
	}

	ConfigKey key {filesystem_name, bucket, operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		if (value == 0) {
			return;
		}
		OperationConfig config;
		config.filesystem_name = filesystem_name;
		config.bucket = bucket;
		config.operation = operation;
		config.quota = 0;
		config.mode = RateLimitMode::BLOCKING;
		config.burst = value;
		it = configs.emplace(key, config).first;
	} else {
		it->second.burst = value;
		if (it->second.IsEmpty()) {
			configs.erase(it);
			return;
		}
	}

	UpdateRateLimiter(it->second);
}

void RateLimitConfig::SetMaxRequestsBucket(const string &filesystem_name, const string &bucket,
                                           FileSystemOperation operation, int64_t value) {
	if (value < CountingSemaphore::UNLIMITED) {
		throw InvalidInputException("Max requests value must be -1 (unlimited) or a positive integer, got %lld", value);
	}
	if (value == 0) {
		throw InvalidInputException("Max requests value cannot be 0 (would block all requests)");
	}

	ConfigKey key {filesystem_name, bucket, operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	auto it = configs.find(key);
	if (it == configs.end()) {
		if (value == CountingSemaphore::UNLIMITED) {
			return;
		}
		OperationConfig config;
		config.filesystem_name = filesystem_name;
		config.bucket = bucket;
		config.operation = operation;
		config.max_requests = value;
		config.semaphore = make_shared_ptr<CountingSemaphore>(value);
		it = configs.emplace(key, config).first;
	} else {
		it->second.max_requests = value;
		if (it->second.IsEmpty()) {
			configs.erase(it);
			return;
		}
		if (value == CountingSemaphore::UNLIMITED) {
			it->second.semaphore = nullptr;
		} else if (it->second.semaphore) {
			it->second.semaphore->SetMax(value);
		} else {
			it->second.semaphore = make_shared_ptr<CountingSemaphore>(value);
		}
	}
}

void RateLimitConfig::ClearConfigBucket(const string &filesystem_name, const string &bucket,
                                        FileSystemOperation operation) {
	ConfigKey key {filesystem_name, bucket, operation};
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	configs.erase(key);
}

void RateLimitConfig::ClearFilesystemBucket(const string &filesystem_name, const string &bucket) {
	concurrency::lock_guard<concurrency::mutex> guard(config_lock);
	for (auto it = configs.begin(); it != configs.end();) {
		if (it->first.filesystem_name == filesystem_name && it->first.bucket == bucket) {
			it = configs.erase(it);
		} else {
			++it;
		}
	}
}

string RateLimitConfig::ExtractBucket(const string &path) {
	try {
		auto parsed_path = Path::FromString(path);
		return parsed_path.GetAuthority();
	} catch (...) {
		return "";
	}
}

RateLimitConfig::RateLimitSnapshot RateLimitConfig::GetRateLimitSnapshotForPath(const string &filesystem_name,
                                                                                const string &path,
                                                                                FileSystemOperation operation) {
	string bucket = ExtractBucket(path);

	concurrency::lock_guard<concurrency::mutex> guard(config_lock);

	// Try bucket-specific config first (if bucket is not empty)
	if (!bucket.empty()) {
		ConfigKey bucket_key {filesystem_name, bucket, operation};
		auto it = configs.find(bucket_key);
		if (it != configs.end()) {
			RateLimitSnapshot snapshot;
			auto &op_config = it->second;
			snapshot.mode = op_config.mode;

			if (op_config.quota > 0 || op_config.burst > 0) {
				if (!op_config.rate_limiter) {
					UpdateRateLimiter(op_config);
				}
				snapshot.rate_limiter = op_config.rate_limiter;
			}

			if (op_config.max_requests != CountingSemaphore::UNLIMITED) {
				D_ASSERT(op_config.semaphore);
				snapshot.semaphore = op_config.semaphore;
			}

			return snapshot;
		}
	}

	// Fallback to filesystem-level config
	ConfigKey fs_key {filesystem_name, "", operation};
	auto it = configs.find(fs_key);
	if (it == configs.end()) {
		return RateLimitSnapshot();
	}

	RateLimitSnapshot snapshot;
	auto &op_config = it->second;
	snapshot.mode = op_config.mode;

	if (op_config.quota > 0 || op_config.burst > 0) {
		if (!op_config.rate_limiter) {
			UpdateRateLimiter(op_config);
		}
		snapshot.rate_limiter = op_config.rate_limiter;
	}

	if (op_config.max_requests != CountingSemaphore::UNLIMITED) {
		D_ASSERT(op_config.semaphore);
		snapshot.semaphore = op_config.semaphore;
	}

	return snapshot;
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

	// Set the database instance for logging
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
		throw InternalException("Database instance is no longer available");
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
	D_ASSERT(!config.IsEmpty());

	if (config.quota == 0 && config.burst == 0) {
		config.rate_limiter = nullptr;
		return;
	}

	config.rate_limiter = CreateRateLimiter(config.quota, config.burst, clock);
}

} // namespace duckdb
