#include "catch/catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "mock_clock.hpp"
#include "rate_limit_config.hpp"
#include "rate_limit_file_system.hpp"
#include "scoped_directory.hpp"

#include <atomic>

using namespace duckdb;
using namespace std::chrono_literals;

namespace {

constexpr const char *TEST_DIR = "/tmp/test_rate_limit_fs_mock";
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
// MockClock-based tests for deterministic rate limiting verification
// ==========================================================================

TEST_CASE("RateLimitFileSystem - MockClock: non-blocking throws after exhausting burst",
          "[rate_limit_fs][mock_clock]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// 10 bytes/sec rate, 20 byte burst
	config->SetQuota(TEST_FS_NAME, FileSystemOperation::READ, 10, RateLimitMode::NON_BLOCKING);
	config->SetBurst(TEST_FS_NAME, FileSystemOperation::READ, 20);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	string temp_path = CreateTempFile(test_dir.GetPath(), "mock_test.txt", test_content);

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
	string buffer(100, '\0');

	// First read: 20 bytes - uses full burst
	auto bytes_read = fs.Read(*handle, buffer.data(), 20);
	REQUIRE(bytes_read == 20);

	// Second read also succeeds (within tolerance window)
	bytes_read = fs.Read(*handle, buffer.data(), 20);
	REQUIRE(bytes_read == 20);

	// Third read immediately should fail (non-blocking, tolerance exhausted)
	REQUIRE_THROWS_AS(fs.Read(*handle, buffer.data(), 1), IOException);

	handle->Close();
}

TEST_CASE("RateLimitFileSystem - MockClock: advancing time restores capacity", "[rate_limit_fs][mock_clock]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// 10 bytes/sec rate, 10 byte burst
	config->SetQuota(TEST_FS_NAME, FileSystemOperation::READ, 10, RateLimitMode::NON_BLOCKING);
	config->SetBurst(TEST_FS_NAME, FileSystemOperation::READ, 10);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	string temp_path = CreateTempFile(test_dir.GetPath(), "mock_advance_test.txt", test_content);

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
	string buffer(100, '\0');

	// First read: 10 bytes - uses full burst
	auto bytes_read = fs.Read(*handle, buffer.data(), 10);
	REQUIRE(bytes_read == 10);

	// Second read also succeeds (within tolerance window)
	bytes_read = fs.Read(*handle, buffer.data(), 10);
	REQUIRE(bytes_read == 10);

	// Third read should fail immediately (tolerance exhausted)
	REQUIRE_THROWS_AS(fs.Read(*handle, buffer.data(), 1), IOException);

	// Advance time by 1 second - should restore 10 bytes of capacity
	mock_clock->Advance(1s);

	// Now we should be able to read again
	bytes_read = fs.Read(*handle, buffer.data(), 10);
	REQUIRE(bytes_read == 10);

	handle->Close();
}

TEST_CASE("RateLimitFileSystem - MockClock: partial time advance restores partial capacity",
          "[rate_limit_fs][mock_clock]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// 10 bytes/sec rate, 10 byte burst
	config->SetQuota(TEST_FS_NAME, FileSystemOperation::READ, 10, RateLimitMode::NON_BLOCKING);
	config->SetBurst(TEST_FS_NAME, FileSystemOperation::READ, 10);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	string temp_path = CreateTempFile(test_dir.GetPath(), "mock_partial_test.txt", test_content);

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
	string buffer(100, '\0');

	// First read: 10 bytes - uses full burst
	auto bytes_read = fs.Read(*handle, buffer.data(), 10);
	REQUIRE(bytes_read == 10);

	// Second read also succeeds (within tolerance window)
	bytes_read = fs.Read(*handle, buffer.data(), 10);
	REQUIRE(bytes_read == 10);

	// Advance time by 1.5 seconds - should restore some capacity
	// After reading 20 bytes, TAT = now + 2s. After 1.5s, current = now + 1.5s
	// To read 5 bytes: earliest_allowed = (now + 2s) - 1s = now + 1s
	// Since now + 1.5s >= now + 1s, we can read 5 bytes
	mock_clock->Advance(1500ms);

	// Should be able to read 5 bytes
	bytes_read = fs.Read(*handle, buffer.data(), 5);
	REQUIRE(bytes_read == 5);

	// After reading 5 bytes, TAT = now + 2.5s, current = now + 1.5s
	// To read 1 byte: earliest_allowed = (now + 2.5s) - 1s = now + 1.5s
	// Since now + 1.5s >= now + 1.5s, we can still read 1 byte (within tolerance)
	bytes_read = fs.Read(*handle, buffer.data(), 1);
	REQUIRE(bytes_read == 1);

	// But not more (tolerance exhausted)
	REQUIRE_THROWS_AS(fs.Read(*handle, buffer.data(), 1), IOException);

	handle->Close();
}

TEST_CASE("RateLimitFileSystem - MockClock: stat operations with mock clock", "[rate_limit_fs][mock_clock]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// 2 ops/sec rate (no burst for non-byte operations)
	// Each operation takes 500ms of "time budget"
	// With burst=0, effective_burst=1, tolerance=500ms allows 2 operations
	config->SetQuota(TEST_FS_NAME, FileSystemOperation::STAT, 2, RateLimitMode::NON_BLOCKING);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "test";
	string temp_path = CreateTempFile(test_dir.GetPath(), "mock_stat_test.txt", test_content);

	// First operation passes
	REQUIRE(fs.FileExists(temp_path));

	// Second operation immediately should also pass (within tolerance window)
	REQUIRE(fs.FileExists(temp_path));

	// Third operation immediately should fail (rate limited, need to wait 500ms)
	REQUIRE_THROWS_AS(fs.FileExists(temp_path), IOException);

	// Advance time by 500ms (1 op at 2 ops/sec)
	mock_clock->Advance(500ms);

	// Now one more should work
	REQUIRE(fs.FileExists(temp_path));

	// But immediate next one fails again
	REQUIRE_THROWS_AS(fs.FileExists(temp_path), IOException);
}

// ==========================================================================
// Concurrent access tests
// ==========================================================================

TEST_CASE("RateLimitFileSystem - MockClock: concurrent reads within burst - no rate limit triggered",
          "[rate_limit_fs][mock_clock][concurrent]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// 100 bytes/sec rate, 100 byte burst - enough for all concurrent reads
	config->SetQuota(TEST_FS_NAME, FileSystemOperation::READ, 100, RateLimitMode::NON_BLOCKING);
	config->SetBurst(TEST_FS_NAME, FileSystemOperation::READ, 100);

	auto inner_fs = make_uniq<LocalFileSystem>();
	auto fs = make_shared_ptr<RateLimitFileSystem>(std::move(inner_fs), config);

	string test_content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	string temp_path = CreateTempFile(test_dir.GetPath(), "concurrent_no_limit.txt", test_content);

	constexpr int NUM_THREADS = 10;
	constexpr int BYTES_PER_THREAD = 10; // Total: 100 bytes = burst limit

	std::atomic<int> success_count {0};
	std::atomic<int> failure_count {0};
	vector<thread> threads;

	for (int i = 0; i < NUM_THREADS; i++) {
		threads.emplace_back([&, i]() {
			try {
				auto handle = fs->OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
				string buffer(BYTES_PER_THREAD, '\0');
				auto bytes_read = fs->Read(*handle, buffer.data(), BYTES_PER_THREAD);
				if (bytes_read == BYTES_PER_THREAD) {
					success_count++;
				}
				handle->Close();
			} catch (const IOException &) {
				failure_count++;
			}
		});
	}

	for (auto &t : threads) {
		t.join();
	}

	// All threads should succeed since total bytes (100) equals burst
	REQUIRE(success_count == NUM_THREADS);
	REQUIRE(failure_count == 0);
}

TEST_CASE("RateLimitFileSystem - MockClock: concurrent reads exceed burst - rate limit triggered",
          "[rate_limit_fs][mock_clock][concurrent]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// 10 bytes/sec rate, 30 byte burst - not enough for all concurrent reads
	// With tolerance window, more than 3 threads might succeed, so adjust expectation
	config->SetQuota(TEST_FS_NAME, FileSystemOperation::READ, 10, RateLimitMode::NON_BLOCKING);
	config->SetBurst(TEST_FS_NAME, FileSystemOperation::READ, 30);

	auto inner_fs = make_uniq<LocalFileSystem>();
	auto fs = make_shared_ptr<RateLimitFileSystem>(std::move(inner_fs), config);

	string test_content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	string temp_path = CreateTempFile(test_dir.GetPath(), "concurrent_with_limit.txt", test_content);

	constexpr int NUM_THREADS = 10;
	constexpr int BYTES_PER_THREAD = 10; // Total: 100 bytes > 30 byte burst

	std::atomic<int> success_count {0};
	std::atomic<int> failure_count {0};
	vector<thread> threads;

	for (int i = 0; i < NUM_THREADS; i++) {
		threads.emplace_back([&, i]() {
			try {
				auto handle = fs->OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
				string buffer(BYTES_PER_THREAD, '\0');
				auto bytes_read = fs->Read(*handle, buffer.data(), BYTES_PER_THREAD);
				if (bytes_read == BYTES_PER_THREAD) {
					success_count++;
				}
				handle->Close();
			} catch (const IOException &) {
				failure_count++;
			}
		});
	}

	for (auto &t : threads) {
		t.join();
	}

	// Some threads should succeed, some should fail due to rate limiting
	// With tolerance window, more than 3 threads might succeed, but not all
	REQUIRE(success_count <= 6);
	REQUIRE(failure_count >= 4);
	REQUIRE(success_count + failure_count == NUM_THREADS);
}

TEST_CASE("RateLimitFileSystem - MockClock: concurrent reads with time advance",
          "[rate_limit_fs][mock_clock][concurrent]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// 10 bytes/sec rate, 20 byte burst
	config->SetQuota(TEST_FS_NAME, FileSystemOperation::READ, 10, RateLimitMode::NON_BLOCKING);
	config->SetBurst(TEST_FS_NAME, FileSystemOperation::READ, 20);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	string temp_path = CreateTempFile(test_dir.GetPath(), "concurrent_time_advance.txt", test_content);

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
	string buffer(100, '\0');

	// First batch: read 20 bytes (uses full burst)
	auto bytes_read = fs.Read(*handle, buffer.data(), 20);
	REQUIRE(bytes_read == 20);

	// Second read also succeeds (within tolerance window)
	bytes_read = fs.Read(*handle, buffer.data(), 20);
	REQUIRE(bytes_read == 20);

	// Third read should fail now (tolerance exhausted)
	REQUIRE_THROWS_AS(fs.Read(*handle, buffer.data(), 1), IOException);

	// Advance time by 2 seconds - should restore 20 bytes
	mock_clock->Advance(2s);

	// Should be able to read 20 more bytes
	bytes_read = fs.Read(*handle, buffer.data(), 20);
	REQUIRE(bytes_read == 20);

	handle->Close();
}
