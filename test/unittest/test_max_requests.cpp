#include "catch/catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "rate_limit_config.hpp"
#include "rate_limit_file_system.hpp"
#include "scoped_directory.hpp"

#include <atomic>
#include <thread>
#include <vector>

using namespace duckdb;

namespace {

constexpr const char *TEST_DIR = "/tmp/test_max_requests";
constexpr const char *TEST_FS_NAME = "RateLimitFileSystem - LocalFileSystem";

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

TEST_CASE("MaxRequests - config stored and retrieved", "[max_requests]") {
	auto config = make_shared_ptr<RateLimitConfig>();

	config->SetMaxRequests(TEST_FS_NAME, FileSystemOperation::READ, 5);
	auto op_config = config->GetConfig(TEST_FS_NAME, FileSystemOperation::READ);
	REQUIRE(op_config != nullptr);
	REQUIRE(op_config->max_requests == 5);
}

TEST_CASE("MaxRequests - reset to -1 keeps config if rate limit exists", "[max_requests]") {
	auto config = make_shared_ptr<RateLimitConfig>();

	config->SetQuota(TEST_FS_NAME, FileSystemOperation::READ, 1000, RateLimitMode::BLOCKING);
	config->SetMaxRequests(TEST_FS_NAME, FileSystemOperation::READ, 5);

	config->SetMaxRequests(TEST_FS_NAME, FileSystemOperation::READ, -1);
	auto op_config = config->GetConfig(TEST_FS_NAME, FileSystemOperation::READ);
	REQUIRE(op_config != nullptr);
	REQUIRE(op_config->quota == 1000);
	REQUIRE(op_config->max_requests == -1);
}

TEST_CASE("MaxRequests - operations work with concurrency limit", "[max_requests]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetMaxRequests(TEST_FS_NAME, FileSystemOperation::STAT, 2);

	auto inner_fs = make_uniq<LocalFileSystem>();
	RateLimitFileSystem fs(std::move(inner_fs), config);

	string temp_path = CreateTempFile(test_dir.GetPath(), "test.txt", "Hello");
	REQUIRE(fs.FileExists(temp_path));
	REQUIRE(fs.FileExists(temp_path));
}

TEST_CASE("MaxRequests - limit=1 serializes operations", "[max_requests]") {
	ScopedDirectory test_dir(TEST_DIR);

	auto config = make_shared_ptr<RateLimitConfig>();
	config->SetMaxRequests(TEST_FS_NAME, FileSystemOperation::STAT, 1);

	auto inner_fs = make_uniq<LocalFileSystem>();
	auto fs = make_shared_ptr<RateLimitFileSystem>(std::move(inner_fs), config);

	string temp_path = CreateTempFile(test_dir.GetPath(), "serial_test.txt", "Hello");

	std::atomic<int64_t> concurrent_count {0};
	std::atomic<bool> violation {false};

	auto worker = [&] {
		for (int i = 0; i < 5; i++) {
			if (++concurrent_count > 1) {
				violation.store(true);
			}
			[[maybe_unused]] auto exists = fs->FileExists(temp_path);
			--concurrent_count;
		}
	};

	std::vector<std::thread> threads;
	for (int i = 0; i < 4; i++) {
		threads.emplace_back(worker);
	}
	for (auto &t : threads) {
		t.join();
	}
	REQUIRE_FALSE(violation.load());
}
