#include "catch/catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "mock_clock.hpp"
#include "path_util.hpp"
#include "rate_limit_config.hpp"
#include "rate_limit_file_system.hpp"
#include "scoped_directory.hpp"

using namespace duckdb;
using namespace std::chrono_literals;

namespace {

constexpr const char *TEST_DIR = "/tmp/test_rate_limit_per_bucket";
constexpr const char *TEST_FS_NAME = "RateLimitFileSystem - LocalFileSystem";

// Helper to create a temporary file with content inside the test directory
string CreateTempFile(const string &dir, const string &filename, const string &content) {
	string path = StringUtil::Format("%s/%s", dir, filename);
	LocalFileSystem fs;
	auto handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	fs.Write(*handle, const_cast<char *>(content.c_str()), static_cast<int64_t>(content.size()));
	handle->Sync();
	handle->Close();
	return path;
}

} // namespace

// ==========================================================================
// Per-Bucket Rate Limiting Tests
// ==========================================================================

TEST_CASE("PerBucketRateLimit - Bucket extraction from S3 paths", "[rate_limit_fs][per_bucket]") {
	// Basic bucket extraction is tested in test_path_util.cpp
	// Here we just verify integration with config
	auto config = make_shared_ptr<RateLimitConfig>();

	// Set bucket-specific limit
	config->SetQuotaBucket(TEST_FS_NAME, "my-bucket", FileSystemOperation::READ, 100, RateLimitMode::BLOCKING);

	// Verify we can get snapshot for that bucket
	auto snapshot =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "s3://my-bucket/file.txt", FileSystemOperation::READ);
	REQUIRE(snapshot.rate_limiter != nullptr);
}

TEST_CASE("PerBucketRateLimit - Bucket-specific limit overrides filesystem limit", "[rate_limit_fs][per_bucket]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// Filesystem-level limit: 5 bytes/sec
	config->SetQuota(TEST_FS_NAME, FileSystemOperation::READ, 5, RateLimitMode::NON_BLOCKING);
	config->SetBurst(TEST_FS_NAME, FileSystemOperation::READ, 5);

	// Bucket-specific limit: 20 bytes/sec (higher than filesystem)
	config->SetQuotaBucket(TEST_FS_NAME, "fast-bucket", FileSystemOperation::READ, 20, RateLimitMode::NON_BLOCKING);
	config->SetBurstBucket(TEST_FS_NAME, "fast-bucket", FileSystemOperation::READ, 20);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	string temp_path = CreateTempFile(test_dir.GetPath(), "test.txt", test_content);

	// Simulate an S3 path by creating a path that would extract "fast-bucket"
	// In reality, we're testing with local files, but the rate limiter uses the path string
	// Since local paths return empty bucket, let's test the config directly

	// Get snapshot for "fast-bucket" - should use bucket-specific config
	auto snapshot_bucket =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "s3://fast-bucket/file.txt", FileSystemOperation::READ);
	REQUIRE(snapshot_bucket.rate_limiter != nullptr);
	REQUIRE(snapshot_bucket.mode == RateLimitMode::NON_BLOCKING);

	// Verify that the bucket limit allows 20 bytes (not the 5 byte filesystem limit)
	auto result = snapshot_bucket.rate_limiter->TryAcquireImmediate(20);
	REQUIRE(!result.has_value()); // Should succeed

	// Get snapshot for unknown bucket - should fall back to filesystem limit
	auto snapshot_fallback =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "s3://unknown/file.txt", FileSystemOperation::READ);
	REQUIRE(snapshot_fallback.rate_limiter != nullptr);

	auto result_fallback = snapshot_fallback.rate_limiter->TryAcquireImmediate(20);
	REQUIRE(result_fallback.has_value()); // Should fail - exceeds 5 byte burst
}

TEST_CASE("PerBucketRateLimit - Multiple buckets with different limits", "[rate_limit_fs][per_bucket][mock_clock]") {
	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// No filesystem-level limit

	// Bucket A: 10 bytes/sec
	config->SetQuotaBucket(TEST_FS_NAME, "bucket-a", FileSystemOperation::READ, 10, RateLimitMode::NON_BLOCKING);
	config->SetBurstBucket(TEST_FS_NAME, "bucket-a", FileSystemOperation::READ, 10);

	// Bucket B: 30 bytes/sec
	config->SetQuotaBucket(TEST_FS_NAME, "bucket-b", FileSystemOperation::READ, 30, RateLimitMode::NON_BLOCKING);
	config->SetBurstBucket(TEST_FS_NAME, "bucket-b", FileSystemOperation::READ, 30);

	// Test bucket A
	auto snapshot_a =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "s3://bucket-a/file.txt", FileSystemOperation::READ);
	REQUIRE(snapshot_a.rate_limiter != nullptr);

	// Can read 10 bytes
	auto result_a = snapshot_a.rate_limiter->TryAcquireImmediate(10);
	REQUIRE(!result_a.has_value());

	// Can read another 10 bytes (within tolerance window)
	result_a = snapshot_a.rate_limiter->TryAcquireImmediate(10);
	REQUIRE(!result_a.has_value());

	// Third request should fail (tolerance exhausted)
	result_a = snapshot_a.rate_limiter->TryAcquireImmediate(1);
	REQUIRE(result_a.has_value());

	// Test bucket B
	auto snapshot_b =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "s3://bucket-b/file.txt", FileSystemOperation::READ);
	REQUIRE(snapshot_b.rate_limiter != nullptr);

	// Can read 30 bytes
	auto result_b = snapshot_b.rate_limiter->TryAcquireImmediate(30);
	REQUIRE(!result_b.has_value());

	// Can read another 30 bytes (within tolerance window)
	result_b = snapshot_b.rate_limiter->TryAcquireImmediate(30);
	REQUIRE(!result_b.has_value());

	// Third request should fail (tolerance exhausted)
	result_b = snapshot_b.rate_limiter->TryAcquireImmediate(1);
	REQUIRE(result_b.has_value());
}

TEST_CASE("PerBucketRateLimit - Fallback to filesystem limit when no bucket config", "[rate_limit_fs][per_bucket]") {
	auto config = make_shared_ptr<RateLimitConfig>();

	// Set only filesystem-level limit
	config->SetQuota(TEST_FS_NAME, FileSystemOperation::READ, 100, RateLimitMode::BLOCKING);
	config->SetBurst(TEST_FS_NAME, FileSystemOperation::READ, 100);

	// Try to get config for a specific bucket - should fall back
	auto snapshot =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "s3://any-bucket/file.txt", FileSystemOperation::READ);

	REQUIRE(snapshot.rate_limiter != nullptr);
	REQUIRE(snapshot.mode == RateLimitMode::BLOCKING);

	// Verify it uses filesystem limit (100 bytes burst)
	auto result = snapshot.rate_limiter->TryAcquireImmediate(100);
	REQUIRE(!result.has_value()); // Should succeed
}

TEST_CASE("PerBucketRateLimit - Local paths use filesystem-level limits", "[rate_limit_fs][per_bucket]") {
	auto config = make_shared_ptr<RateLimitConfig>();

	// Set filesystem-level limit
	config->SetQuota(TEST_FS_NAME, FileSystemOperation::READ, 50, RateLimitMode::NON_BLOCKING);
	config->SetBurst(TEST_FS_NAME, FileSystemOperation::READ, 50);

	// Set bucket-specific limit (should not apply to local paths)
	config->SetQuotaBucket(TEST_FS_NAME, "some-bucket", FileSystemOperation::READ, 200, RateLimitMode::NON_BLOCKING);
	config->SetBurstBucket(TEST_FS_NAME, "some-bucket", FileSystemOperation::READ, 200);

	// Local path - should use filesystem limit
	auto snapshot =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "/local/path/file.txt", FileSystemOperation::READ);

	REQUIRE(snapshot.rate_limiter != nullptr);

	// Should use 50 byte limit, not 200
	auto result = snapshot.rate_limiter->TryAcquireImmediate(50);
	REQUIRE(!result.has_value()); // Succeeds with 50

	auto result2 = snapshot.rate_limiter->TryAcquireImmediate(100);
	REQUIRE(result2.has_value()); // Fails with 100 (exceeds burst)
}

TEST_CASE("PerBucketRateLimit - GetAllConfigs includes bucket information", "[rate_limit_fs][per_bucket]") {
	auto config = make_shared_ptr<RateLimitConfig>();

	// Add filesystem-level config
	config->SetQuota(TEST_FS_NAME, FileSystemOperation::READ, 100, RateLimitMode::BLOCKING);

	// Add bucket-specific configs
	config->SetQuotaBucket(TEST_FS_NAME, "bucket-1", FileSystemOperation::READ, 200, RateLimitMode::NON_BLOCKING);
	config->SetQuotaBucket(TEST_FS_NAME, "bucket-2", FileSystemOperation::WRITE, 300, RateLimitMode::BLOCKING);

	auto all_configs = config->GetAllConfigs();

	REQUIRE(all_configs.size() == 3);

	// Find each config and verify
	bool found_fs_level = false;
	bool found_bucket1 = false;
	bool found_bucket2 = false;

	for (const auto &cfg : all_configs) {
		if (cfg.bucket.empty() && cfg.operation == FileSystemOperation::READ) {
			found_fs_level = true;
			REQUIRE(cfg.quota == 100);
		} else if (cfg.bucket == "bucket-1" && cfg.operation == FileSystemOperation::READ) {
			found_bucket1 = true;
			REQUIRE(cfg.quota == 200);
			REQUIRE(cfg.mode == RateLimitMode::NON_BLOCKING);
		} else if (cfg.bucket == "bucket-2" && cfg.operation == FileSystemOperation::WRITE) {
			found_bucket2 = true;
			REQUIRE(cfg.quota == 300);
			REQUIRE(cfg.mode == RateLimitMode::BLOCKING);
		}
	}

	REQUIRE(found_fs_level);
	REQUIRE(found_bucket1);
	REQUIRE(found_bucket2);
}

TEST_CASE("PerBucketRateLimit - Clear bucket-specific config", "[rate_limit_fs][per_bucket]") {
	auto config = make_shared_ptr<RateLimitConfig>();

	// Add configs
	config->SetQuotaBucket(TEST_FS_NAME, "bucket-1", FileSystemOperation::READ, 100, RateLimitMode::BLOCKING);
	config->SetQuotaBucket(TEST_FS_NAME, "bucket-1", FileSystemOperation::WRITE, 200, RateLimitMode::BLOCKING);
	config->SetQuotaBucket(TEST_FS_NAME, "bucket-2", FileSystemOperation::READ, 300, RateLimitMode::BLOCKING);

	// Clear one operation for bucket-1
	config->ClearConfigBucket(TEST_FS_NAME, "bucket-1", FileSystemOperation::READ);

	auto all_configs = config->GetAllConfigs();
	REQUIRE(all_configs.size() == 2); // Only WRITE for bucket-1 and READ for bucket-2 remain

	// Clear all operations for bucket-1
	config->ClearFilesystemBucket(TEST_FS_NAME, "bucket-1");

	all_configs = config->GetAllConfigs();
	REQUIRE(all_configs.size() == 1); // Only READ for bucket-2 remains
	REQUIRE(all_configs[0].bucket == "bucket-2");
}

TEST_CASE("PerBucketRateLimit - Max requests (concurrency) per bucket", "[rate_limit_fs][per_bucket]") {
	auto config = make_shared_ptr<RateLimitConfig>();

	// Set bucket-specific max requests
	config->SetMaxRequestsBucket(TEST_FS_NAME, "bucket-a", FileSystemOperation::READ, 5);
	config->SetMaxRequestsBucket(TEST_FS_NAME, "bucket-b", FileSystemOperation::READ, 10);

	// Get snapshot for bucket-a
	auto snapshot_a =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "s3://bucket-a/file.txt", FileSystemOperation::READ);
	REQUIRE(snapshot_a.semaphore != nullptr);

	// Get snapshot for bucket-b
	auto snapshot_b =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "s3://bucket-b/file.txt", FileSystemOperation::READ);
	REQUIRE(snapshot_b.semaphore != nullptr);

	// Verify they are different semaphores
	REQUIRE(snapshot_a.semaphore != snapshot_b.semaphore);
}

TEST_CASE("PerBucketRateLimit - Bucket-specific burst limits for READ and WRITE",
          "[rate_limit_fs][per_bucket][mock_clock]") {
	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// Bucket with small burst for reads
	config->SetQuotaBucket(TEST_FS_NAME, "small-burst", FileSystemOperation::READ, 100, RateLimitMode::NON_BLOCKING);
	config->SetBurstBucket(TEST_FS_NAME, "small-burst", FileSystemOperation::READ, 10);

	// Bucket with large burst for writes
	config->SetQuotaBucket(TEST_FS_NAME, "large-burst", FileSystemOperation::WRITE, 100, RateLimitMode::NON_BLOCKING);
	config->SetBurstBucket(TEST_FS_NAME, "large-burst", FileSystemOperation::WRITE, 1000);

	// Test small burst
	auto snapshot_small =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "s3://small-burst/file.txt", FileSystemOperation::READ);
	REQUIRE(snapshot_small.rate_limiter != nullptr);

	auto result_small = snapshot_small.rate_limiter->TryAcquireImmediate(10);
	REQUIRE(!result_small.has_value()); // 10 bytes succeeds

	auto result_small_fail = snapshot_small.rate_limiter->TryAcquireImmediate(11);
	REQUIRE(result_small_fail.has_value()); // 11 bytes fails (exceeds burst capacity)

	// Test large burst
	auto snapshot_large =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "s3://large-burst/file.txt", FileSystemOperation::WRITE);
	REQUIRE(snapshot_large.rate_limiter != nullptr);

	auto result_large = snapshot_large.rate_limiter->TryAcquireImmediate(1000);
	REQUIRE(!result_large.has_value()); // 1000 bytes succeeds

	auto result_large_fail = snapshot_large.rate_limiter->TryAcquireImmediate(1001);
	REQUIRE(result_large_fail.has_value()); // 1001 bytes fails
}

TEST_CASE("PerBucketRateLimit - Time-based refill with mock clock", "[rate_limit_fs][per_bucket][mock_clock]") {
	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// 10 bytes/sec, 10 byte burst
	config->SetQuotaBucket(TEST_FS_NAME, "test-bucket", FileSystemOperation::READ, 10, RateLimitMode::NON_BLOCKING);
	config->SetBurstBucket(TEST_FS_NAME, "test-bucket", FileSystemOperation::READ, 10);

	auto snapshot =
	    config->GetRateLimitSnapshotForPath(TEST_FS_NAME, "s3://test-bucket/file.txt", FileSystemOperation::READ);
	REQUIRE(snapshot.rate_limiter != nullptr);

	// Consume full burst
	auto result1 = snapshot.rate_limiter->TryAcquireImmediate(10);
	REQUIRE(!result1.has_value());

	// Second request also succeeds (within tolerance window)
	auto result2 = snapshot.rate_limiter->TryAcquireImmediate(10);
	REQUIRE(!result2.has_value());

	// Third request fails immediately (tolerance exhausted)
	auto result3 = snapshot.rate_limiter->TryAcquireImmediate(1);
	REQUIRE(result3.has_value());

	// Advance time by 1 second (should refill 10 bytes)
	mock_clock->Advance(1s);

	// Now the request should succeed
	auto result4 = snapshot.rate_limiter->TryAcquireImmediate(10);
	REQUIRE(!result4.has_value());
}
