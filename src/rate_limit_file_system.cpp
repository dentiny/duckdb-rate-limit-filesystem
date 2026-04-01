#include "rate_limit_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

// ==========================================================================
// RateLimitFileHandle
// ==========================================================================

RateLimitFileHandle::RateLimitFileHandle(RateLimitFileSystem &fs, unique_ptr<FileHandle> inner_handle_p,
                                         const string &path, FileOpenFlags flags)
    : FileHandle(fs, path, flags), inner_handle(std::move(inner_handle_p)) {
}

RateLimitFileHandle::~RateLimitFileHandle() {
}

void RateLimitFileHandle::Close() {
	if (inner_handle) {
		inner_handle->Close();
	}
}

FileHandle &RateLimitFileHandle::GetInnerHandle() {
	return *inner_handle;
}

// ==========================================================================
// RateLimitFileSystem
// ==========================================================================

RateLimitFileSystem::RateLimitFileSystem(unique_ptr<FileSystem> inner_fs_p, shared_ptr<RateLimitConfig> config_p)
    : filesystem_name(StringUtil::Format("RateLimitFileSystem - %s", inner_fs_p->GetName())),
      inner_fs(std::move(inner_fs_p)), config(std::move(config_p)) {
	if (!config) {
		throw InvalidInputException("RateLimitFileSystem requires a non-null RateLimitConfig");
	}
}

RateLimitFileSystem::~RateLimitFileSystem() {
}

void RateLimitFileSystem::ApplyRateLimit(FileSystemOperation operation, idx_t bytes) {
	auto snapshot = config->GetRateLimitSnapshot(filesystem_name, operation);
	if (!snapshot.rate_limiter) {
		return;
	}

	if (snapshot.mode == RateLimitMode::NONE) {
		throw InternalException("Rate limit mode is NONE for operation '%s', which should not happen",
		                        FileSystemOperationToString(operation));
	}

	// Non-blocking mode: check if we can acquire immediately, throw if not
	if (snapshot.mode == RateLimitMode::NON_BLOCKING) {
		auto result = snapshot.rate_limiter->TryAcquireImmediate(bytes);
		// Allowed immediately
		if (!result.has_value()) {
			return;
		}

		// Check if burst capacity is exceeded
		if (result->wait_duration == Duration::max()) {
			throw IOException("Request size %llu exceeds burst capacity for operation '%s'", bytes,
			                  FileSystemOperationToString(operation));
		}

		// Rate limit exceeded, throw immediately
		throw IOException("Rate limit exceeded for operation '%s': would need to wait %lld ms",
		                  FileSystemOperationToString(operation),
		                  std::chrono::duration_cast<std::chrono::milliseconds>(result->wait_duration).count());
	}

	// Blocking mode: wait until ready
	D_ASSERT(snapshot.mode == RateLimitMode::BLOCKING);
	auto wait_result = snapshot.rate_limiter->UntilNReady(bytes);
	if (wait_result == RateLimitResult::InsufficientCapacity) {
		throw IOException("Request size %llu exceeds burst capacity for operation '%s'", bytes,
		                  FileSystemOperationToString(operation));
	}
}

FileHandle &RateLimitFileSystem::GetInnerFileHandle(FileHandle &handle) {
	auto &rate_limit_handle = handle.Cast<RateLimitFileHandle>();
	return rate_limit_handle.GetInnerHandle();
}

SemaphoreGuard RateLimitFileSystem::AcquireConcurrencySlot(FileSystemOperation operation) {
	auto semaphore = config->GetOrCreateSemaphore(filesystem_name, operation);
	if (!semaphore) {
		return SemaphoreGuard();
	}
	return semaphore->AcquireGuard();
}

// ==========================================================================
// Rate limited operations
// ==========================================================================

unique_ptr<FileHandle> RateLimitFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                     optional_ptr<FileOpener> opener) {
	return OpenFileExtended(OpenFileInfo(path), flags, opener);
}

unique_ptr<FileHandle> RateLimitFileSystem::OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
                                                             optional_ptr<FileOpener> opener) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::STAT);
	ApplyRateLimit(FileSystemOperation::STAT);
	auto inner_handle = inner_fs->OpenFile(file, flags, opener);
	if (!inner_handle) {
		return nullptr;
	}
	return make_uniq<RateLimitFileHandle>(*this, std::move(inner_handle), file.path, flags);
}

void RateLimitFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::READ);
	auto &inner_handle = GetInnerFileHandle(handle);
	auto file_size = inner_fs->GetFileSize(inner_handle);
	const idx_t actual_bytes = MinValue<idx_t>(static_cast<idx_t>(nr_bytes), static_cast<idx_t>(file_size) - location);
	ApplyRateLimit(FileSystemOperation::READ, actual_bytes);
	inner_fs->Read(inner_handle, buffer, nr_bytes, location);
}

// TODO: Consider how multipart upload interacts with concurrency limits.
// A single Write at the filesystem level may fan out into multiple HTTP PUT requests
// for large payloads, which means the actual TCP connection count could exceed the limit.
void RateLimitFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::WRITE);
	ApplyRateLimit(FileSystemOperation::WRITE, static_cast<idx_t>(nr_bytes));
	inner_fs->Write(GetInnerFileHandle(handle), buffer, nr_bytes, location);
}

int64_t RateLimitFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::READ);
	ApplyRateLimit(FileSystemOperation::READ, static_cast<idx_t>(nr_bytes));
	return inner_fs->Read(GetInnerFileHandle(handle), buffer, nr_bytes);
}

int64_t RateLimitFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::WRITE);
	ApplyRateLimit(FileSystemOperation::WRITE, static_cast<idx_t>(nr_bytes));
	return inner_fs->Write(GetInnerFileHandle(handle), buffer, nr_bytes);
}

FileMetadata RateLimitFileSystem::Stats(FileHandle &handle) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::STAT);
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->Stats(GetInnerFileHandle(handle));
}

int64_t RateLimitFileSystem::GetFileSize(FileHandle &handle) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::STAT);
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->GetFileSize(GetInnerFileHandle(handle));
}

timestamp_t RateLimitFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::STAT);
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->GetLastModifiedTime(GetInnerFileHandle(handle));
}

FileType RateLimitFileSystem::GetFileType(FileHandle &handle) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::STAT);
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->GetFileType(GetInnerFileHandle(handle));
}

string RateLimitFileSystem::GetVersionTag(FileHandle &handle) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::STAT);
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->GetVersionTag(GetInnerFileHandle(handle));
}

void RateLimitFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::WRITE);
	ApplyRateLimit(FileSystemOperation::WRITE);
	inner_fs->Truncate(GetInnerFileHandle(handle), new_size);
}

bool RateLimitFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::STAT);
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->DirectoryExists(directory, opener);
}

void RateLimitFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::DELETE);
	ApplyRateLimit(FileSystemOperation::DELETE);
	inner_fs->RemoveDirectory(directory, opener);
}

void RateLimitFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	inner_fs->MoveFile(source, target, opener);
}

bool RateLimitFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::STAT);
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->FileExists(filename, opener);
}

void RateLimitFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::DELETE);
	ApplyRateLimit(FileSystemOperation::DELETE);
	inner_fs->RemoveFile(filename, opener);
}

bool RateLimitFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::DELETE);
	ApplyRateLimit(FileSystemOperation::DELETE);
	return inner_fs->TryRemoveFile(filename, opener);
}

void RateLimitFileSystem::RemoveFiles(const vector<string> &filenames, optional_ptr<FileOpener> opener) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::DELETE);
	ApplyRateLimit(FileSystemOperation::DELETE);
	inner_fs->RemoveFiles(filenames, opener);
}

vector<OpenFileInfo> RateLimitFileSystem::Glob(const string &path, FileOpener *opener) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::LIST);
	ApplyRateLimit(FileSystemOperation::LIST);
	auto result = inner_fs->Glob(path, FileGlobOptions::ALLOW_EMPTY, opener);
	return result->GetAllFiles();
}

bool RateLimitFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                                    FileOpener *opener) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::LIST);
	ApplyRateLimit(FileSystemOperation::LIST);
	return inner_fs->ListFiles(directory, callback, opener);
}

bool RateLimitFileSystem::ListFilesExtended(const string &directory,
                                            const std::function<void(OpenFileInfo &info)> &callback,
                                            optional_ptr<FileOpener> opener) {
	auto concurrency_guard = AcquireConcurrencySlot(FileSystemOperation::LIST);
	ApplyRateLimit(FileSystemOperation::LIST);
	return inner_fs->ListFiles(directory, callback, opener);
}

// ==========================================================================
// Delegate to inner file system (no rate limiting)
// ==========================================================================

bool RateLimitFileSystem::SupportsOpenFileExtended() const {
	return true;
}

bool RateLimitFileSystem::SupportsListFilesExtended() const {
	return true;
}

void RateLimitFileSystem::FileSync(FileHandle &handle) {
	inner_fs->FileSync(GetInnerFileHandle(handle));
}

void RateLimitFileSystem::Seek(FileHandle &handle, idx_t location) {
	inner_fs->Seek(GetInnerFileHandle(handle), location);
}

void RateLimitFileSystem::Reset(FileHandle &handle) {
	inner_fs->Reset(GetInnerFileHandle(handle));
}

idx_t RateLimitFileSystem::SeekPosition(FileHandle &handle) {
	return inner_fs->SeekPosition(GetInnerFileHandle(handle));
}

bool RateLimitFileSystem::CanHandleFile(const string &path) {
	return inner_fs->CanHandleFile(path);
}

bool RateLimitFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return inner_fs->IsPipe(filename, opener);
}

bool RateLimitFileSystem::CanSeek() {
	return inner_fs->CanSeek();
}

void RateLimitFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	inner_fs->CreateDirectory(directory, opener);
}

bool RateLimitFileSystem::OnDiskFile(FileHandle &handle) {
	return inner_fs->OnDiskFile(GetInnerFileHandle(handle));
}

string RateLimitFileSystem::GetName() const {
	return filesystem_name;
}

string RateLimitFileSystem::PathSeparator(const string &path) {
	return inner_fs->PathSeparator(path);
}

} // namespace duckdb
