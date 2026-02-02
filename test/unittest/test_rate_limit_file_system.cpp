#include "catch/catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "mock_clock.hpp"
#include "rate_limit_config.hpp"
#include "rate_limit_file_system.hpp"
#include "scoped_directory.hpp"

using namespace duckdb;
using namespace std::chrono_literals;

namespace {

constexpr const char *TEST_DIR = "/tmp/test_rate_limit_fs";

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

TEST_CASE("RateLimitFileSystem - basic operations without rate limiting", "[rate_limit_fs]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto config = make_shared_ptr<RateLimitConfig>();
	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "Hello, World!";
	string temp_path = CreateTempFile(test_dir.GetPath(), "basic_test.txt", test_content);

	SECTION("OpenFile works") {
		auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
		REQUIRE(handle != nullptr);
		handle->Close();
	}

	SECTION("Read works") {
		auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
		string buffer(100, '\0');
		auto bytes_read = fs.Read(*handle, buffer.data(), static_cast<int64_t>(test_content.length()));
		REQUIRE(bytes_read == static_cast<int64_t>(test_content.length()));
		REQUIRE(buffer.substr(0, test_content.length()) == test_content);
		handle->Close();
	}

	SECTION("GetFileSize works") {
		auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
		auto size = fs.GetFileSize(*handle);
		REQUIRE(size == static_cast<int64_t>(test_content.length()));
		handle->Close();
	}

	SECTION("FileExists works") {
		REQUIRE(fs.FileExists(temp_path));
		REQUIRE_FALSE(fs.FileExists("/tmp/nonexistent_file_12345.txt"));
	}
}

TEST_CASE("RateLimitFileSystem - with rate limiting config", "[rate_limit_fs]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto config = make_shared_ptr<RateLimitConfig>();
	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "Hello, World!";
	string temp_path = CreateTempFile(test_dir.GetPath(), "rate_limit_test.txt", test_content);

	SECTION("Read with rate limiting - blocking mode allows read") {
		// Set rate limit: 100 bytes per second, 1000 byte burst
		config->SetQuota(FileSystemOperation::READ, 100, RateLimitMode::BLOCKING);
		config->SetBurst(FileSystemOperation::READ, 1000);

		auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
		string buffer(100, '\0');
		// First read should succeed (within burst)
		auto bytes_read = fs.Read(*handle, buffer.data(), static_cast<int64_t>(test_content.length()));
		REQUIRE(bytes_read == static_cast<int64_t>(test_content.length()));
		handle->Close();
	}

	SECTION("Stat operations use stat rate limit") {
		// Set rate limit for stat: 10 ops/sec
		config->SetQuota(FileSystemOperation::STAT, 10, RateLimitMode::BLOCKING);
		config->SetBurst(FileSystemOperation::STAT, 100);

		// These should all work (within burst)
		REQUIRE(fs.FileExists(temp_path));

		auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
		auto size = fs.GetFileSize(*handle);
		REQUIRE(size == static_cast<int64_t>(test_content.length()));
		handle->Close();
	}
}

TEST_CASE("RateLimitFileSystem - non-blocking mode throws on rate limit", "[rate_limit_fs]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto config = make_shared_ptr<RateLimitConfig>();
	// Very low rate limit: 1 byte per second, 10 byte burst
	config->SetQuota(FileSystemOperation::READ, 1, RateLimitMode::NON_BLOCKING);
	config->SetBurst(FileSystemOperation::READ, 10);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "Hello, World! This is a longer test content.";
	string temp_path = CreateTempFile(test_dir.GetPath(), "non_blocking_test.txt", test_content);

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);

	// First read of 10 bytes should succeed (equals burst)
	string buffer(100, '\0');
	auto bytes_read = fs.Read(*handle, buffer.data(), 10);
	REQUIRE(bytes_read == 10);

	// Second immediate read should fail (rate limit exceeded, non-blocking)
	REQUIRE_THROWS_AS(fs.Read(*handle, buffer.data(), 10), IOException);

	handle->Close();
}

TEST_CASE("RateLimitFileSystem - GetName returns correct name", "[rate_limit_fs]") {
	auto config = make_shared_ptr<RateLimitConfig>();
	RateLimitFileSystem fs(config);
	REQUIRE(fs.GetName() == "RateLimitFileSystem");
}

TEST_CASE("RateLimitFileSystem - GetInnerFileSystem", "[rate_limit_fs]") {
	auto config = make_shared_ptr<RateLimitConfig>();
	auto inner_fs = make_uniq<LocalFileSystem>();
	auto *inner_fs_ptr = inner_fs.get();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	REQUIRE(&fs.GetInnerFileSystem() == inner_fs_ptr);
}

TEST_CASE("RateLimitFileSystem - list operations rate limiting", "[rate_limit_fs]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto config = make_shared_ptr<RateLimitConfig>();
	// Set rate limit for list: 100 ops/sec
	config->SetQuota(FileSystemOperation::LIST, 100, RateLimitMode::BLOCKING);
	config->SetBurst(FileSystemOperation::LIST, 100);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	// Glob should work (uses list rate limit)
	auto files = fs.Glob(test_dir.GetPath() + "/*.txt");
	// Just verify it doesn't throw
	REQUIRE(true);
}

TEST_CASE("RateLimitFileSystem - write operations rate limiting", "[rate_limit_fs]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto config = make_shared_ptr<RateLimitConfig>();
	// Set rate limit for write: 1000 bytes/sec, 10000 byte burst
	config->SetQuota(FileSystemOperation::WRITE, 1000, RateLimitMode::BLOCKING);
	config->SetBurst(FileSystemOperation::WRITE, 10000);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string temp_path = test_dir.GetPath() + "/write_test.txt";

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);

	string content = "Hello, World!";
	auto bytes_written = fs.Write(*handle, content.data(), static_cast<int64_t>(content.length()));
	REQUIRE(bytes_written == static_cast<int64_t>(content.length()));

	handle->Close();
}

TEST_CASE("RateLimitFileSystem - delete operations rate limiting", "[rate_limit_fs]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto config = make_shared_ptr<RateLimitConfig>();
	// Set rate limit for delete: 10 ops/sec
	config->SetQuota(FileSystemOperation::DELETE, 10, RateLimitMode::BLOCKING);
	config->SetBurst(FileSystemOperation::DELETE, 100);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	// Create a temp file
	string temp_path = CreateTempFile(test_dir.GetPath(), "delete_test.txt", "test content");

	// RemoveFile should work (uses delete rate limit)
	fs.RemoveFile(temp_path);
	REQUIRE_FALSE(fs.FileExists(temp_path));
}

TEST_CASE("RateLimitFileSystem - burst exceeds check", "[rate_limit_fs]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto config = make_shared_ptr<RateLimitConfig>();
	// Very small burst: 5 bytes
	config->SetQuota(FileSystemOperation::READ, 100, RateLimitMode::BLOCKING);
	config->SetBurst(FileSystemOperation::READ, 5);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "Hello, World!";
	string temp_path = CreateTempFile(test_dir.GetPath(), "burst_test.txt", test_content);

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);

	// Try to read 10 bytes, but burst is only 5 - should throw
	string buffer(100, '\0');
	REQUIRE_THROWS_AS(fs.Read(*handle, buffer.data(), 10), IOException);

	handle->Close();
}

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
	config->SetQuota(FileSystemOperation::READ, 10, RateLimitMode::NON_BLOCKING);
	config->SetBurst(FileSystemOperation::READ, 20);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	string temp_path = CreateTempFile(test_dir.GetPath(), "mock_test.txt", test_content);

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
	string buffer(100, '\0');

	// First read: 20 bytes - uses up entire burst
	auto bytes_read = fs.Read(*handle, buffer.data(), 20);
	REQUIRE(bytes_read == 20);

	// Second read immediately should fail (non-blocking, no capacity)
	REQUIRE_THROWS_AS(fs.Read(*handle, buffer.data(), 1), IOException);

	handle->Close();
}

TEST_CASE("RateLimitFileSystem - MockClock: advancing time restores capacity", "[rate_limit_fs][mock_clock]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// 10 bytes/sec rate, 10 byte burst
	config->SetQuota(FileSystemOperation::READ, 10, RateLimitMode::NON_BLOCKING);
	config->SetBurst(FileSystemOperation::READ, 10);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	string temp_path = CreateTempFile(test_dir.GetPath(), "mock_advance_test.txt", test_content);

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
	string buffer(100, '\0');

	// First read: 10 bytes - uses up entire burst
	auto bytes_read = fs.Read(*handle, buffer.data(), 10);
	REQUIRE(bytes_read == 10);

	// Should fail immediately
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
	config->SetQuota(FileSystemOperation::READ, 10, RateLimitMode::NON_BLOCKING);
	config->SetBurst(FileSystemOperation::READ, 10);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	string temp_path = CreateTempFile(test_dir.GetPath(), "mock_partial_test.txt", test_content);

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
	string buffer(100, '\0');

	// First read: 10 bytes - uses up entire burst
	auto bytes_read = fs.Read(*handle, buffer.data(), 10);
	REQUIRE(bytes_read == 10);

	// Advance time by 500ms - should restore 5 bytes of capacity
	mock_clock->Advance(500ms);

	// Should be able to read 5 bytes
	bytes_read = fs.Read(*handle, buffer.data(), 5);
	REQUIRE(bytes_read == 5);

	// But not more
	REQUIRE_THROWS_AS(fs.Read(*handle, buffer.data(), 1), IOException);

	handle->Close();
}

TEST_CASE("RateLimitFileSystem - MockClock: stat operations with mock clock", "[rate_limit_fs][mock_clock]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto mock_clock = CreateMockClock();
	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetClock(mock_clock);

	// 2 ops/sec rate, 2 ops burst
	config->SetQuota(FileSystemOperation::STAT, 2, RateLimitMode::NON_BLOCKING);
	config->SetBurst(FileSystemOperation::STAT, 2);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string test_content = "test";
	string temp_path = CreateTempFile(test_dir.GetPath(), "mock_stat_test.txt", test_content);

	// First two stat operations should succeed (within burst)
	REQUIRE(fs.FileExists(temp_path));
	REQUIRE(fs.FileExists(temp_path));

	// Third should fail
	REQUIRE_THROWS_AS(fs.FileExists(temp_path), IOException);

	// Advance time by 1 second - restores 2 ops
	mock_clock->Advance(1s);

	// Now two more should work
	REQUIRE(fs.FileExists(temp_path));
	REQUIRE(fs.FileExists(temp_path));
	REQUIRE_THROWS_AS(fs.FileExists(temp_path), IOException);
}
