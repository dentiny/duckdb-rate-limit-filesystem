#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Scalar function: rate_limit_fs_quota(operation VARCHAR, value BIGINT, mode VARCHAR)
// Sets the rate limit quota (bandwidth) for an operation.
// - operation: The operation name (e.g., 'read', 'write', 'list')
// - value: The quota value in bytes per second. 0 to disable rate limiting for this operation.
// - mode: 'blocking' (wait until allowed) or 'non_blocking' (fail immediately if exceeded)
// Returns the operation name on success.
ScalarFunction GetRateLimitFsQuotaFunction();

// Scalar function: rate_limit_fs_burst(operation VARCHAR, value BIGINT)
// Sets the burst limit for an operation.
// - operation: The operation name (e.g., 'read', 'write', 'list')
// - value: The burst value. 0 to disable burst limiting for this operation.
// Returns the operation name on success.
ScalarFunction GetRateLimitFsBurstFunction();

// Scalar function: rate_limit_fs_clear(operation VARCHAR)
// Clears the rate limit configuration for an operation.
// - operation: The operation name to clear, or '*' to clear all.
// Returns the operation name(s) cleared.
ScalarFunction GetRateLimitFsClearFunction();

// Table function: rate_limit_fs_configs()
// Returns all configured rate limit settings.
// Columns: operation VARCHAR, quota BIGINT, mode VARCHAR, burst BIGINT
TableFunction GetRateLimitFsConfigsFunction();

} // namespace duckdb

