#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Scalar function: rate_limit_fs_quota(filesystem_name VARCHAR, operation VARCHAR, value BIGINT, mode VARCHAR) ->
// BOOLEAN Sets the rate limit quota (bandwidth) for an operation on a specific filesystem.
// - filesystem_name: The filesystem name to configure (e.g., 'LocalFileSystem')
// - operation: The operation name (e.g., 'read', 'write', 'list')
// - value: The quota value in bytes per second. 0 to disable rate limiting for this operation.
// - mode: 'blocking' (wait until allowed) or 'non_blocking' (fail immediately if exceeded)
// Returns true on success.
ScalarFunction GetRateLimitFsQuotaFunction();

// Scalar function: rate_limit_fs_burst(filesystem_name VARCHAR, operation VARCHAR, value BIGINT) -> BOOLEAN
// Sets the burst limit for an operation on a specific filesystem.
// - filesystem_name: The filesystem name to configure (e.g., 'LocalFileSystem')
// - operation: The operation name (e.g., 'read', 'write')
// - value: The burst value. 0 to disable burst limiting for this operation.
// Returns true on success.
ScalarFunction GetRateLimitFsBurstFunction();

// Scalar function: rate_limit_fs_clear(filesystem_name VARCHAR, operation VARCHAR) -> BOOLEAN
// Clears the rate limit configuration for an operation on a specific filesystem.
// - filesystem_name: The filesystem name, or '*' to clear all filesystems.
// - operation: The operation name to clear, or '*' to clear all operations.
// Returns true on success.
ScalarFunction GetRateLimitFsClearFunction();

// Table function: rate_limit_fs_configs()
// Returns all configured rate limit settings.
// Columns: filesystem VARCHAR, operation VARCHAR, quota BIGINT, mode VARCHAR, burst BIGINT
TableFunction GetRateLimitFsConfigsFunction();

// Table function: rate_limit_fs_list_filesystems()
// Lists all registered filesystems in the virtual file system.
// Columns: name VARCHAR
TableFunction GetRateLimitFsListFilesystemsFunction();

// rate_limit_fs_max_requests(filesystem_name, operation, value) -> BOOLEAN
// value: -1 for unlimited (default), or a positive integer for the concurrency cap.
ScalarFunction GetRateLimitFsMaxRequestsFunction();

// Scalar function: rate_limit_fs_wrap(filesystem_name VARCHAR) -> BOOLEAN
// Extracts the specified filesystem from the virtual filesystem registry,
// wraps it with the rate limit filesystem, and registers the wrapped version.
// - filesystem_name: The name of the filesystem to wrap
// Returns true on success.
ScalarFunction GetRateLimitFsWrapFunction();

// Bucket-specific rate limiting functions

// Scalar function: rate_limit_fs_quota_bucket(filesystem_name VARCHAR, bucket VARCHAR, operation VARCHAR, value BIGINT,
// mode VARCHAR) -> BOOLEAN Sets the rate limit quota for a specific bucket on a filesystem.
// - filesystem_name: The filesystem name to configure
// - bucket: The bucket name (e.g., S3 bucket name)
// - operation: The operation name (e.g., 'read', 'write', 'list')
// - value: The quota value in bytes per second
// - mode: 'blocking' or 'non_blocking'
// Returns true on success.
ScalarFunction GetRateLimitFsQuotaBucketFunction();

// Scalar function: rate_limit_fs_burst_bucket(filesystem_name VARCHAR, bucket VARCHAR, operation VARCHAR, value BIGINT)
// -> BOOLEAN Sets the burst limit for a specific bucket on a filesystem.
// - filesystem_name: The filesystem name to configure
// - bucket: The bucket name
// - operation: The operation name (e.g., 'read', 'write')
// - value: The burst value in bytes
// Returns true on success.
ScalarFunction GetRateLimitFsBurstBucketFunction();

// Scalar function: rate_limit_fs_max_requests_bucket(filesystem_name VARCHAR, bucket VARCHAR, operation VARCHAR, value
// BIGINT) -> BOOLEAN Sets the max concurrent requests for a specific bucket on a filesystem.
// - filesystem_name: The filesystem name to configure
// - bucket: The bucket name
// - operation: The operation name
// - value: -1 for unlimited, or a positive integer for the concurrency cap
// Returns true on success.
ScalarFunction GetRateLimitFsMaxRequestsBucketFunction();

// Scalar function: rate_limit_fs_clear_bucket(filesystem_name VARCHAR, bucket VARCHAR, operation VARCHAR) -> BOOLEAN
// Clears the rate limit configuration for a specific bucket.
// - filesystem_name: The filesystem name
// - bucket: The bucket name
// - operation: The operation name to clear, or '*' to clear all operations for the bucket
// Returns true on success.
ScalarFunction GetRateLimitFsClearBucketFunction();

} // namespace duckdb
