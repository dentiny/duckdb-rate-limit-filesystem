#include "catch/catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "mock_clock.hpp"
#include "rate_limit_config.hpp"
#include "rate_limit_file_system.hpp"

#include <chrono>
#include <cstring>
#include <fstream>

using namespace duckdb;

namespace {

// Helper to create a temporary file with content
string CreateTempFile(const string &content) {
	static int counter = 0;
	string path = "/tmp/test_rate_limit_fs_" + std::to_string(counter++) + ".txt";
	std::ofstream file(path);
	file << content;
	file.close();
	return path;
}

// Helper to clean up temp file
void RemoveTempFile(const string &path) {
	std::remove(path.c_str());
}

} // namespace

TEST_CASE("RateLimitFileSystem - basic operations without rate limiting", "[rate_limit_fs]") {
	auto inner_fs = make_shared_ptr<LocalFileSystem>();
	RateLimitFileSystem fs(inner_fs);

	string test_content = "Hello, World!";
	string temp_path = CreateTempFile(test_content);

	SECTION("OpenFile works without config") {
		auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
		REQUIRE(handle != nullptr);
		handle->Close();
	}

	SECTION("Read works without config") {
		auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
		char buffer[100];
		auto bytes_read = fs.Read(*handle, buffer, 13);
		REQUIRE(bytes_read == 13);
		buffer[13] = '\0';
		REQUIRE(string(buffer) == test_content);
		handle->Close();
	}

	SECTION("GetFileSize works without config") {
		auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
		auto size = fs.GetFileSize(*handle);
		REQUIRE(size == 13);
		handle->Close();
	}

	SECTION("FileExists works without config") {
		REQUIRE(fs.FileExists(temp_path));
		REQUIRE_FALSE(fs.FileExists("/tmp/nonexistent_file_12345.txt"));
	}

	RemoveTempFile(temp_path);
}

TEST_CASE("RateLimitFileSystem - with rate limiting config", "[rate_limit_fs]") {
	auto inner_fs = make_shared_ptr<LocalFileSystem>();
	RateLimitFileSystem fs(inner_fs);

	auto config = make_shared_ptr<RateLimitConfig>();
	fs.SetConfig(config);

	string test_content = "Hello, World!";
	string temp_path = CreateTempFile(test_content);

	SECTION("Read with rate limiting - blocking mode allows read") {
		// Set rate limit: 100 bytes per second, 1000 byte burst
		config->SetQuota(FileSystemOperation::READ, 100, RateLimitMode::BLOCKING);
		config->SetBurst(FileSystemOperation::READ, 1000);

		auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);
		char buffer[100];
		// First read should succeed (within burst)
		auto bytes_read = fs.Read(*handle, buffer, 13);
		REQUIRE(bytes_read == 13);
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
		REQUIRE(size == 13);
		handle->Close();
	}

	RemoveTempFile(temp_path);
}

TEST_CASE("RateLimitFileSystem - non-blocking mode throws on rate limit", "[rate_limit_fs]") {
	auto mock_clock = CreateMockClock();
	auto inner_fs = make_shared_ptr<LocalFileSystem>();
	RateLimitFileSystem fs(inner_fs);

	auto config = make_shared_ptr<RateLimitConfig>();
	fs.SetConfig(config);

	// Very low rate limit: 1 byte per second, 10 byte burst
	config->SetQuota(FileSystemOperation::READ, 1, RateLimitMode::NON_BLOCKING);
	config->SetBurst(FileSystemOperation::READ, 10);

	string test_content = "Hello, World! This is a longer test content.";
	string temp_path = CreateTempFile(test_content);

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);

	// First read of 10 bytes should succeed (equals burst)
	char buffer[100];
	auto bytes_read = fs.Read(*handle, buffer, 10);
	REQUIRE(bytes_read == 10);

	// Second immediate read should fail (rate limit exceeded, non-blocking)
	REQUIRE_THROWS_AS(fs.Read(*handle, buffer, 10), IOException);

	handle->Close();
	RemoveTempFile(temp_path);
}

TEST_CASE("RateLimitFileSystem - GetName returns correct name", "[rate_limit_fs]") {
	RateLimitFileSystem fs;
	REQUIRE(fs.GetName() == "RateLimitFileSystem");
}

TEST_CASE("RateLimitFileSystem - GetConfig and SetConfig", "[rate_limit_fs]") {
	RateLimitFileSystem fs;

	REQUIRE(fs.GetConfig() == nullptr);

	auto config = make_shared_ptr<RateLimitConfig>();
	fs.SetConfig(config);
	REQUIRE(fs.GetConfig() == config);

	fs.SetConfig(nullptr);
	REQUIRE(fs.GetConfig() == nullptr);
}

TEST_CASE("RateLimitFileSystem - GetInnerFileSystem", "[rate_limit_fs]") {
	auto inner_fs = make_shared_ptr<LocalFileSystem>();
	RateLimitFileSystem fs(inner_fs);

	REQUIRE(&fs.GetInnerFileSystem() == inner_fs.get());
}

TEST_CASE("RateLimitFileSystem - list operations rate limiting", "[rate_limit_fs]") {
	auto inner_fs = make_shared_ptr<LocalFileSystem>();
	RateLimitFileSystem fs(inner_fs);

	auto config = make_shared_ptr<RateLimitConfig>();
	fs.SetConfig(config);

	// Set rate limit for list: 100 ops/sec
	config->SetQuota(FileSystemOperation::LIST, 100, RateLimitMode::BLOCKING);
	config->SetBurst(FileSystemOperation::LIST, 100);

	// Glob should work (uses list rate limit)
	auto files = fs.Glob("/tmp/*.txt");
	// Just verify it doesn't throw
	REQUIRE(true);
}

TEST_CASE("RateLimitFileSystem - write operations rate limiting", "[rate_limit_fs]") {
	auto inner_fs = make_shared_ptr<LocalFileSystem>();
	RateLimitFileSystem fs(inner_fs);

	auto config = make_shared_ptr<RateLimitConfig>();
	fs.SetConfig(config);

	// Set rate limit for write: 1000 bytes/sec, 10000 byte burst
	config->SetQuota(FileSystemOperation::WRITE, 1000, RateLimitMode::BLOCKING);
	config->SetBurst(FileSystemOperation::WRITE, 10000);

	string temp_path = "/tmp/test_rate_limit_write.txt";

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);

	const char *content = "Hello, World!";
	auto bytes_written = fs.Write(*handle, const_cast<char *>(content), 13);
	REQUIRE(bytes_written == 13);

	handle->Close();
	RemoveTempFile(temp_path);
}

TEST_CASE("RateLimitFileSystem - delete operations rate limiting", "[rate_limit_fs]") {
	auto inner_fs = make_shared_ptr<LocalFileSystem>();
	RateLimitFileSystem fs(inner_fs);

	auto config = make_shared_ptr<RateLimitConfig>();
	fs.SetConfig(config);

	// Set rate limit for delete: 10 ops/sec
	config->SetQuota(FileSystemOperation::DELETE, 10, RateLimitMode::BLOCKING);
	config->SetBurst(FileSystemOperation::DELETE, 100);

	// Create a temp file
	string temp_path = CreateTempFile("test content");

	// RemoveFile should work (uses delete rate limit)
	fs.RemoveFile(temp_path);
	REQUIRE_FALSE(fs.FileExists(temp_path));
}

TEST_CASE("RateLimitFileSystem - burst exceeds check", "[rate_limit_fs]") {
	auto inner_fs = make_shared_ptr<LocalFileSystem>();
	RateLimitFileSystem fs(inner_fs);

	auto config = make_shared_ptr<RateLimitConfig>();
	fs.SetConfig(config);

	// Very small burst: 5 bytes
	config->SetQuota(FileSystemOperation::READ, 100, RateLimitMode::BLOCKING);
	config->SetBurst(FileSystemOperation::READ, 5);

	string test_content = "Hello, World!";
	string temp_path = CreateTempFile(test_content);

	auto handle = fs.OpenFile(temp_path, FileOpenFlags::FILE_FLAGS_READ);

	// Try to read 10 bytes, but burst is only 5 - should throw
	char buffer[100];
	REQUIRE_THROWS_AS(fs.Read(*handle, buffer, 10), IOException);

	handle->Close();
	RemoveTempFile(temp_path);
}
