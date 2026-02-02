#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/object_cache.hpp"

#include "base_clock.hpp"
#include "file_system_operation.hpp"
#include "mutex.hpp"
#include "rate_limit_mode.hpp"
#include "rate_limiter.hpp"

namespace duckdb {

// Configuration for a single operation's rate limiting.
struct OperationConfig {
	// Inner filesystem name (internal filesystem name) this config belongs to.
	string filesystem_name;
	// Operation type
	FileSystemOperation operation;
	// Quota value (bandwidth in bytes per second)
	idx_t quota;
	// Behavior mode when rate limit is exceeded
	RateLimitMode mode;
	// Burst value (0 means no burst limiting)
	idx_t burst;
	// The rate limiter instance (created lazily)
	SharedRateLimiter rate_limiter;

	OperationConfig()
	    : filesystem_name(), operation(FileSystemOperation::NONE), quota(0), mode(RateLimitMode::NONE), burst(0),
	      rate_limiter(nullptr) {
	}
};

// Per-DuckDB-instance rate limit configuration storage.
class RateLimitConfig : public ObjectCacheEntry {
public:
	static constexpr const char *OBJECT_TYPE = "rate_limit_config";
	static constexpr const char *CACHE_KEY = "rate_limit_fs_config";

	RateLimitConfig();
	~RateLimitConfig() override;

	// Returns the object type identifier.
	string GetObjectType() override;

	// Static method for ObjectCache::Get compatibility.
	static string ObjectType();

	// Sets the quota for an operation on a specific filesystem.
	void SetQuota(const string &filesystem_name, FileSystemOperation operation, idx_t value, RateLimitMode mode);

	// Sets the burst for an operation on a specific filesystem.
	void SetBurst(const string &filesystem_name, FileSystemOperation operation, idx_t value);

	// Gets the configuration for an operation on a specific filesystem. Returns nullptr if not configured.
	const OperationConfig *GetConfig(const string &filesystem_name, FileSystemOperation operation) const;

	// Gets or creates a rate limiter for an operation on a specific filesystem. Returns nullptr if not configured.
	SharedRateLimiter GetOrCreateRateLimiter(const string &filesystem_name, FileSystemOperation operation);

	// Returns all configured operations across all filesystems.
	vector<OperationConfig> GetAllConfigs() const;

	// Returns all configured operations for a specific filesystem.
	vector<OperationConfig> GetConfigsForFilesystem(const string &filesystem_name) const;

	// Clears the configuration for an operation on a specific filesystem.
	void ClearConfig(const string &filesystem_name, FileSystemOperation operation);

	// Clears all configurations for a specific filesystem.
	void ClearFilesystem(const string &filesystem_name);

	// Clears all configurations.
	void ClearAll();

	// Gets or creates the config from the client context's object cache.
	static shared_ptr<RateLimitConfig> GetOrCreate(ClientContext &context);

	// Gets the config from the client context's object cache. Returns nullptr if not exists.
	static shared_ptr<RateLimitConfig> Get(ClientContext &context);

	// Sets the clock to use for rate limiters (for testing with MockClock).
	void SetClock(shared_ptr<BaseClock> clock_p);

private:
	// Key for the config map
	struct ConfigKey {
		string filesystem_name;
		FileSystemOperation operation;

		bool operator==(const ConfigKey &other) const {
			return filesystem_name == other.filesystem_name && operation == other.operation;
		}
	};

	struct ConfigKeyHash {
		size_t operator()(const ConfigKey &key) const {
			return std::hash<string>()(key.filesystem_name) ^
			       (std::hash<uint8_t>()(static_cast<uint8_t>(key.operation)) << 1);
		}
	};

	// Updates the rate limiter for an operation based on current config.
	void UpdateRateLimiter(OperationConfig &config) DUCKDB_REQUIRES(config_lock);

	mutable concurrency::mutex config_lock;
	// Maps from (filesystem_name, operation) to its configuration.
	unordered_map<ConfigKey, OperationConfig, ConfigKeyHash> configs DUCKDB_GUARDED_BY(config_lock);
	// Clock to use for rate limiters (nullptr means use default clock).
	shared_ptr<BaseClock> clock DUCKDB_GUARDED_BY(config_lock);
};

} // namespace duckdb
