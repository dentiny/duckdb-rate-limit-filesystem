#include "rate_limit_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/local_file_system.hpp"

#include <thread>

namespace duckdb {

//------------------------------------------------------------------------------
// RateLimitFileHandle
//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------
// RateLimitFileSystem
//------------------------------------------------------------------------------

RateLimitFileSystem::RateLimitFileSystem(shared_ptr<FileSystem> inner_fs_p) : inner_fs(std::move(inner_fs_p)) {
}

RateLimitFileSystem::RateLimitFileSystem() : inner_fs(FileSystem::CreateLocal()) {
}

RateLimitFileSystem::~RateLimitFileSystem() {
}

void RateLimitFileSystem::SetConfig(shared_ptr<RateLimitConfig> config_p) {
	config = std::move(config_p);
}

shared_ptr<RateLimitConfig> RateLimitFileSystem::GetConfig() const {
	return config;
}

FileSystem &RateLimitFileSystem::GetInnerFileSystem() const {
	return *inner_fs;
}

void RateLimitFileSystem::ApplyRateLimit(FileSystemOperation operation, idx_t bytes) {
	if (!config) {
		return;
	}

	auto rate_limiter = config->GetOrCreateRateLimiter(operation);
	if (!rate_limiter) {
		return;
	}

	const auto *op_config = config->GetConfig(operation);
	if (!op_config) {
		return;
	}

	auto result = rate_limiter->TryAcquireImmediate(bytes);
	if (!result.has_value()) {
		// Allowed immediately
		return;
	}

	if (op_config->mode == RateLimitMode::NON_BLOCKING) {
		throw IOException("Rate limit exceeded for operation '%s': would need to wait %lld ms",
		                  FileSystemOperationToString(operation),
		                  std::chrono::duration_cast<std::chrono::milliseconds>(result->wait_duration).count());
	}

	// Blocking mode: wait until ready
	auto wait_result = rate_limiter->UntilNReady(bytes);
	if (wait_result == RateLimitResult::InsufficientCapacity) {
		throw IOException("Request size %llu exceeds burst capacity for operation '%s'", bytes,
		                  FileSystemOperationToString(operation));
	}
}

FileHandle &RateLimitFileSystem::GetInnerFileHandle(FileHandle &handle) {
	auto *rate_limit_handle = dynamic_cast<RateLimitFileHandle *>(&handle);
	if (rate_limit_handle) {
		return rate_limit_handle->GetInnerHandle();
	}
	return handle;
}

unique_ptr<FileHandle> RateLimitFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                     optional_ptr<FileOpener> opener) {
	// Note: OpenFile is not rate limited as it's typically fast and metadata-only
	auto inner_handle = inner_fs->OpenFile(path, flags, opener);
	return make_uniq<RateLimitFileHandle>(*this, std::move(inner_handle), path, flags);
}

void RateLimitFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	ApplyRateLimit(FileSystemOperation::READ, static_cast<idx_t>(nr_bytes));
	inner_fs->Read(GetInnerFileHandle(handle), buffer, nr_bytes, location);
}

void RateLimitFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	ApplyRateLimit(FileSystemOperation::WRITE, static_cast<idx_t>(nr_bytes));
	inner_fs->Write(GetInnerFileHandle(handle), buffer, nr_bytes, location);
}

int64_t RateLimitFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	ApplyRateLimit(FileSystemOperation::READ, static_cast<idx_t>(nr_bytes));
	return inner_fs->Read(GetInnerFileHandle(handle), buffer, nr_bytes);
}

int64_t RateLimitFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	ApplyRateLimit(FileSystemOperation::WRITE, static_cast<idx_t>(nr_bytes));
	return inner_fs->Write(GetInnerFileHandle(handle), buffer, nr_bytes);
}

int64_t RateLimitFileSystem::GetFileSize(FileHandle &handle) {
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->GetFileSize(GetInnerFileHandle(handle));
}

timestamp_t RateLimitFileSystem::GetLastModifiedTime(FileHandle &handle) {
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->GetLastModifiedTime(GetInnerFileHandle(handle));
}

FileType RateLimitFileSystem::GetFileType(FileHandle &handle) {
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->GetFileType(GetInnerFileHandle(handle));
}

void RateLimitFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	ApplyRateLimit(FileSystemOperation::WRITE);
	inner_fs->Truncate(GetInnerFileHandle(handle), new_size);
}

void RateLimitFileSystem::FileSync(FileHandle &handle) {
	inner_fs->FileSync(GetInnerFileHandle(handle));
}

bool RateLimitFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->DirectoryExists(directory, opener);
}

void RateLimitFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	ApplyRateLimit(FileSystemOperation::WRITE);
	inner_fs->CreateDirectory(directory, opener);
}

void RateLimitFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	ApplyRateLimit(FileSystemOperation::DELETE);
	inner_fs->RemoveDirectory(directory, opener);
}

void RateLimitFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	ApplyRateLimit(FileSystemOperation::WRITE);
	inner_fs->MoveFile(source, target, opener);
}

bool RateLimitFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->FileExists(filename, opener);
}

bool RateLimitFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	ApplyRateLimit(FileSystemOperation::STAT);
	return inner_fs->IsPipe(filename, opener);
}

void RateLimitFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	ApplyRateLimit(FileSystemOperation::DELETE);
	inner_fs->RemoveFile(filename, opener);
}

bool RateLimitFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	ApplyRateLimit(FileSystemOperation::DELETE);
	return inner_fs->TryRemoveFile(filename, opener);
}

vector<OpenFileInfo> RateLimitFileSystem::Glob(const string &path, FileOpener *opener) {
	ApplyRateLimit(FileSystemOperation::LIST);
	return inner_fs->Glob(path, opener);
}

bool RateLimitFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                                    FileOpener *opener) {
	ApplyRateLimit(FileSystemOperation::LIST);
	return inner_fs->ListFiles(directory, callback, opener);
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

bool RateLimitFileSystem::CanSeek() {
	return inner_fs->CanSeek();
}

bool RateLimitFileSystem::OnDiskFile(FileHandle &handle) {
	return inner_fs->OnDiskFile(GetInnerFileHandle(handle));
}

string RateLimitFileSystem::GetName() const {
	return "RateLimitFileSystem";
}

string RateLimitFileSystem::PathSeparator(const string &path) {
	return inner_fs->PathSeparator(path);
}

unique_ptr<FileHandle> RateLimitFileSystem::OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
                                                             optional_ptr<FileOpener> opener) {
	// Note: OpenFile is not rate limited as it's typically fast and metadata-only
	auto inner_handle = inner_fs->OpenFile(file, flags, opener);
	return make_uniq<RateLimitFileHandle>(*this, std::move(inner_handle), file.path, flags);
}

bool RateLimitFileSystem::SupportsOpenFileExtended() const {
	return true;
}

bool RateLimitFileSystem::ListFilesExtended(const string &directory,
                                            const std::function<void(OpenFileInfo &info)> &callback,
                                            optional_ptr<FileOpener> opener) {
	ApplyRateLimit(FileSystemOperation::LIST);
	return inner_fs->ListFiles(directory, callback, opener);
}

bool RateLimitFileSystem::SupportsListFilesExtended() const {
	return true;
}

} // namespace duckdb
