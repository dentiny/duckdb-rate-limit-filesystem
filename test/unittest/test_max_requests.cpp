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

class ConcurrencyTrackingFileSystem : public LocalFileSystem {
public:
	std::atomic<int64_t> concurrent_stat_count {0};
	std::atomic<int64_t> max_observed_concurrency {0};

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override {
		auto current = ++concurrent_stat_count;
		auto prev_max = max_observed_concurrency.load();
		while (current > prev_max && !max_observed_concurrency.compare_exchange_weak(prev_max, current)) {
		}
		auto result = LocalFileSystem::FileExists(filename, opener);
		--concurrent_stat_count;
		return result;
	}
};

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

	auto inner_fs = make_uniq<ConcurrencyTrackingFileSystem>();
	auto &tracking = *inner_fs;
	auto fs = make_shared_ptr<RateLimitFileSystem>(std::move(inner_fs), config);

	string temp_path = CreateTempFile(test_dir.GetPath(), "serial_test.txt", "Hello");

	auto worker = [&] {
		for (int i = 0; i < 5; i++) {
			[[maybe_unused]] auto exists = fs->FileExists(temp_path);
		}
	};

	std::vector<std::thread> threads;
	for (int i = 0; i < 4; i++) {
		threads.emplace_back(worker);
	}
	for (auto &t : threads) {
		t.join();
	}

	// The semaphore limits to 1, so at most 1 thread should be inside FileExists at a time.
	REQUIRE(tracking.max_observed_concurrency.load() <= 1);
	REQUIRE(tracking.concurrent_stat_count.load() == 0);
}
