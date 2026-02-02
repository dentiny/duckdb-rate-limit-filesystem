#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Scalar function: rate_limit_fs_quota(filesystem_name VARCHAR, operation VARCHAR, value BIGINT, mode VARCHAR)
// Sets the rate limit quota (bandwidth) for an operation on a specific filesystem.
// - filesystem_name: The filesystem name to configure (e.g., 'LocalFileSystem')
// - operation: The operation name (e.g., 'read', 'write', 'list')
// - value: The quota value in bytes per second. 0 to disable rate limiting for this operation.
// - mode: 'blocking' (wait until allowed) or 'non_blocking' (fail immediately if exceeded)
// Returns the filesystem name on success.
ScalarFunction GetRateLimitFsQuotaFunction();

// Scalar function: rate_limit_fs_burst(filesystem_name VARCHAR, operation VARCHAR, value BIGINT)
// Sets the burst limit for an operation on a specific filesystem.
// - filesystem_name: The filesystem name to configure (e.g., 'LocalFileSystem')
// - operation: The operation name (e.g., 'read', 'write')
// - value: The burst value. 0 to disable burst limiting for this operation.
// Returns the filesystem name on success.
ScalarFunction GetRateLimitFsBurstFunction();

// Scalar function: rate_limit_fs_clear(filesystem_name VARCHAR, operation VARCHAR)
// Clears the rate limit configuration for an operation on a specific filesystem.
// - filesystem_name: The filesystem name, or '*' to clear all filesystems.
// - operation: The operation name to clear, or '*' to clear all operations.
// Returns the filesystem name(s) cleared.
ScalarFunction GetRateLimitFsClearFunction();

// Table function: rate_limit_fs_configs()
// Returns all configured rate limit settings.
// Columns: filesystem VARCHAR, operation VARCHAR, quota BIGINT, mode VARCHAR, burst BIGINT
TableFunction GetRateLimitFsConfigsFunction();

// Table function: rate_limit_fs_list_filesystems()
// Lists all registered filesystems in the virtual file system.
// Columns: name VARCHAR
TableFunction GetRateLimitFsListFilesystemsFunction();

// Scalar function: rate_limit_fs_wrap(filesystem_name VARCHAR)
// Extracts the specified filesystem from the virtual filesystem registry,
// wraps it with the rate limit filesystem, and registers the wrapped version.
// - filesystem_name: The name of the filesystem to wrap
// Returns the new filesystem name on success.
ScalarFunction GetRateLimitFsWrapFunction();

} // namespace duckdb
