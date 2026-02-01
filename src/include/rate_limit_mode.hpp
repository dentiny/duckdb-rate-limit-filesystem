#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

// Represents the behavior mode when rate limit is exceeded.
enum class RateLimitMode : uint8_t {
	// No mode set
	NONE,
	// Wait until the rate limit allows the operation to proceed
	BLOCKING,
	// Fail immediately if the rate limit would be exceeded
	NON_BLOCKING
};

// Converts a string to RateLimitMode. Throws on invalid input.
RateLimitMode ParseRateLimitMode(const string &mode_str);

// Converts RateLimitMode to string.
string RateLimitModeToString(RateLimitMode mode);

} // namespace duckdb
