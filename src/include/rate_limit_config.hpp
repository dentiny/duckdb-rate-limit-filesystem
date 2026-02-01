#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/object_cache.hpp"

#include "rate_limiter.hpp"

namespace duckdb {

// Represents the behavior mode when rate limit is exceeded.
enum class RateLimitMode : uint8_t {
	// Wait until the rate limit allows the operation to proceed
	BLOCKING,
	// Fail immediately if the rate limit would be exceeded
	NON_BLOCKING
};

// Converts a string to RateLimitMode. Throws on invalid input.
RateLimitMode ParseRateLimitMode(const string &mode_str);

// Converts RateLimitMode to string.
string RateLimitModeToString(RateLimitMode mode);

// Configuration for a single operation's rate limiting.
struct OperationConfig {
	// Operation name (e.g., "read", "write", "list")
	string operation;
	// Quota value (bandwidth in bytes per second)
	idx_t quota;
	// Behavior mode when rate limit is exceeded
	RateLimitMode mode;
	// Burst value (0 means no burst limiting)
	idx_t burst;
	// The rate limiter instance (created lazily)
	SharedRateLimiter rate_limiter;

	OperationConfig() : quota(0), mode(RateLimitMode::BLOCKING), burst(0), rate_limiter(nullptr) {
	}
};

// Per-DuckDB-instance rate limit configuration storage.
// Inherits from ObjectCacheEntry for storage in DuckDB's object cache.
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

	// Sets the quota for an operation.
	// If both quota and burst are 0 after this call, the config entry is removed.
	void SetQuota(const string &operation, idx_t value, RateLimitMode mode);

	// Sets the burst for an operation.
	// If both quota and burst are 0 after this call, the config entry is removed.
	void SetBurst(const string &operation, idx_t value);

	// Gets the configuration for an operation. Returns nullptr if not configured.
	const OperationConfig *GetConfig(const string &operation) const;

	// Gets or creates a rate limiter for an operation. Returns nullptr if not configured.
	SharedRateLimiter GetOrCreateRateLimiter(const string &operation);

	// Returns all configured operations.
	vector<OperationConfig> GetAllConfigs() const;

	// Clears the configuration for an operation.
	void ClearConfig(const string &operation);

	// Clears all configurations.
	void ClearAll();

	// Gets or creates the config from the client context's object cache.
	static shared_ptr<RateLimitConfig> GetOrCreate(ClientContext &context);

	// Gets the config from the client context's object cache. Returns nullptr if not exists.
	static shared_ptr<RateLimitConfig> Get(ClientContext &context);

private:
	// Updates the rate limiter for an operation based on current config.
	void UpdateRateLimiter(OperationConfig &config);

	mutable mutex config_lock;
	unordered_map<string, OperationConfig> configs;
};

} // namespace duckdb
