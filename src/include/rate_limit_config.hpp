#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/object_cache.hpp"

#include "base_clock.hpp"
#include "counting_semaphore.hpp"
#include "file_system_operation.hpp"
#include "mutex.hpp"
#include "rate_limit_mode.hpp"
#include "rate_limiter.hpp"

namespace duckdb {

// Forward declaration.
class ClientContext;
class DatabaseInstance;

struct OperationConfig {
	string filesystem_name;
	string bucket;
	FileSystemOperation operation;
	idx_t quota;
	RateLimitMode mode;
	idx_t burst;
	SharedRateLimiter rate_limiter;
	// -1 = unlimited (default), positive = max concurrent operations
	int64_t max_requests;
	shared_ptr<CountingSemaphore> semaphore;

	OperationConfig()
	    : filesystem_name(), bucket(), operation(FileSystemOperation::NONE), quota(0), mode(RateLimitMode::NONE),
	      burst(0), rate_limiter(nullptr), max_requests(CountingSemaphore::UNLIMITED), semaphore(nullptr) {
	}

	bool IsEmpty() const {
		return quota == 0 && burst == 0 && max_requests == CountingSemaphore::UNLIMITED;
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

	optional_idx GetEstimatedCacheMemory() const override {
		// Don't evict this entry.
		return optional_idx {};
	}

	// Sets the quota for an operation on a specific filesystem.
	void SetQuota(const string &filesystem_name, FileSystemOperation operation, idx_t value, RateLimitMode mode);

	// Sets the burst for an operation on a specific filesystem.
	void SetBurst(const string &filesystem_name, FileSystemOperation operation, idx_t value);

	// Sets the max requests for an operation on a specific filesystem.
	void SetMaxRequests(const string &filesystem_name, FileSystemOperation operation, int64_t value);

	// Sets the quota for an operation on a specific filesystem and bucket.
	void SetQuotaBucket(const string &filesystem_name, const string &bucket, FileSystemOperation operation, idx_t value,
	                    RateLimitMode mode);

	// Sets the burst for an operation on a specific filesystem and bucket.
	void SetBurstBucket(const string &filesystem_name, const string &bucket, FileSystemOperation operation,
	                    idx_t value);

	// Sets the max requests for an operation on a specific filesystem and bucket.
	void SetMaxRequestsBucket(const string &filesystem_name, const string &bucket, FileSystemOperation operation,
	                          int64_t value);

	const OperationConfig *GetConfig(const string &filesystem_name, FileSystemOperation operation) const;

	SharedRateLimiter GetOrCreateRateLimiter(const string &filesystem_name, FileSystemOperation operation);

	// Returns nullptr if no limit set.
	shared_ptr<CountingSemaphore> GetOrCreateSemaphore(const string &filesystem_name, FileSystemOperation operation);

	// Snapshot of rate limit info for a single operation, obtained under one lock acquisition.
	// Avoids TOCTOU races and dangling-pointer issues from separate Get*/GetOrCreate* calls.
	struct RateLimitSnapshot {
		SharedRateLimiter rate_limiter;
		shared_ptr<CountingSemaphore> semaphore;
		RateLimitMode mode = RateLimitMode::NONE;
	};

	// Atomically retrieves all rate-limit state needed for a single operation.
	// Returns a snapshot with rate_limiter==nullptr if no config exists for this filesystem/operation.
	RateLimitSnapshot GetRateLimitSnapshot(const string &filesystem_name, FileSystemOperation operation);

	// Atomically retrieves rate-limit state for a specific path (with bucket extraction).
	// Tries bucket-specific config first, then falls back to filesystem-level config.
	RateLimitSnapshot GetRateLimitSnapshotForPath(const string &filesystem_name, const string &path,
	                                              FileSystemOperation operation);

	// Returns all configured operations across all filesystems.
	vector<OperationConfig> GetAllConfigs() const;

	// Returns all configured operations for a specific filesystem.
	vector<OperationConfig> GetConfigsForFilesystem(const string &filesystem_name) const;

	// Clears the configuration for an operation on a specific filesystem.
	void ClearConfig(const string &filesystem_name, FileSystemOperation operation);

	// Clears the configuration for an operation on a specific filesystem and bucket.
	void ClearConfigBucket(const string &filesystem_name, const string &bucket, FileSystemOperation operation);

	// Clears all configurations for a specific filesystem.
	void ClearFilesystem(const string &filesystem_name);

	// Clears all configurations for a specific filesystem and bucket.
	void ClearFilesystemBucket(const string &filesystem_name, const string &bucket);

	// Clears all configurations.
	void ClearAll();

	// Gets or creates the config from the client context's object cache.
	static shared_ptr<RateLimitConfig> GetOrCreate(ClientContext &context);

	// Gets the config from the client context's object cache. Returns nullptr if not exists.
	static shared_ptr<RateLimitConfig> Get(ClientContext &context);

	// Sets the clock to use for rate limiters (for testing with MockClock).
	void SetClock(shared_ptr<BaseClock> clock_p);

	// Gets the database instance for logging.
	// Throws InternalException if the database instance is no longer available.
	shared_ptr<DatabaseInstance> GetDatabaseInstance() const;

private:
	// Key for the config map
	struct ConfigKey {
		string filesystem_name;
		string bucket;
		FileSystemOperation operation;

		bool operator==(const ConfigKey &other) const {
			return filesystem_name == other.filesystem_name && bucket == other.bucket && operation == other.operation;
		}
	};

	struct ConfigKeyHash {
		size_t operator()(const ConfigKey &key) const {
			size_t h1 = std::hash<string>()(key.filesystem_name);
			size_t h2 = std::hash<string>()(key.bucket);
			size_t h3 = std::hash<uint8_t>()(static_cast<uint8_t>(key.operation));
			return h1 ^ (h2 << 1) ^ (h3 << 2);
		}
	};

	// Extracts bucket name from path using DuckDB's Path parser.
	// Returns empty string if no bucket authority is present (e.g., local paths).
	static string ExtractBucket(const string &path);

	// Updates the rate limiter for an operation based on current config.
	void UpdateRateLimiter(OperationConfig &config) DUCKDB_REQUIRES(config_lock);

	mutable concurrency::mutex config_lock;
	// Maps from (filesystem_name, operation) to its configuration.
	unordered_map<ConfigKey, OperationConfig, ConfigKeyHash> configs DUCKDB_GUARDED_BY(config_lock);
	// Clock to use for rate limiters (nullptr means use default clock).
	shared_ptr<BaseClock> clock DUCKDB_GUARDED_BY(config_lock);
	// Weak pointer to database instance for logging, stored as weak pointer to avoid circular references.
	weak_ptr<DatabaseInstance> db_instance DUCKDB_GUARDED_BY(config_lock);
};

} // namespace duckdb
