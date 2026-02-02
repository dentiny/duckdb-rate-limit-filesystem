#include "catch/catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "rate_limit_config.hpp"
#include "rate_limit_file_system.hpp"
#include "scoped_directory.hpp"

using namespace duckdb;

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

TEST_CASE("RateLimitFileSystem - GetName returns correct name", "[rate_limit_fs]") {
	auto config = make_shared_ptr<RateLimitConfig>();
	RateLimitFileSystem fs(config);
	REQUIRE(fs.GetName() == "RateLimitFileSystem - LocalFileSystem");
}

TEST_CASE("RateLimitFileSystem - Rate limit filesystem", "[rate_limit_fs]") {
	auto config = make_shared_ptr<RateLimitConfig>();
	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	// Make sure we can cast to RateLimitFileSystem
	[[maybe_unused]] auto &casted = fs.Cast<RateLimitFileSystem>();
}

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
		// Set rate limit for stat: 10 ops/sec (no burst for non-byte operations)
		config->SetQuota(FileSystemOperation::STAT, 10, RateLimitMode::BLOCKING);

		// These should all work
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

TEST_CASE("RateLimitFileSystem - list operations blocking mode", "[rate_limit_fs]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto config = make_shared_ptr<RateLimitConfig>();
	// Set rate limit for list: 100 ops/sec (no burst for non-byte operations)
	config->SetQuota(FileSystemOperation::LIST, 100, RateLimitMode::BLOCKING);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	// Multiple Glob calls should work (blocking mode waits if needed)
	[[maybe_unused]] auto files1 = fs.Glob(test_dir.GetPath() + "/*.txt");
	[[maybe_unused]] auto files2 = fs.Glob(test_dir.GetPath() + "/*.txt");
}

TEST_CASE("RateLimitFileSystem - list operations non-blocking mode", "[rate_limit_fs]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto config = make_shared_ptr<RateLimitConfig>();
	// Set rate limit for list: 1 op/sec (no burst for non-byte operations)
	// Very low rate to ensure second immediate call exceeds limit
	config->SetQuota(FileSystemOperation::LIST, 1, RateLimitMode::NON_BLOCKING);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	// First Glob should work
	[[maybe_unused]] auto files = fs.Glob(test_dir.GetPath() + "/*.txt");

	// Second immediate Glob should fail (rate limited, non-blocking throws)
	REQUIRE_THROWS_AS(fs.Glob(test_dir.GetPath() + "/*.txt"), IOException);
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
