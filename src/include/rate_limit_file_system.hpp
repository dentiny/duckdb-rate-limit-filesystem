#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"

#include "rate_limit_config.hpp"

namespace duckdb {

// Forward declaration.
class RateLimitFileSystem;

// ==========================================================================
// RateLimitFileHandle
// ==========================================================================

// File handle that wraps another file handle and applies rate limiting.
class RateLimitFileHandle : public FileHandle {
public:
	RateLimitFileHandle(RateLimitFileSystem &fs, unique_ptr<FileHandle> inner_handle_p, const string &path,
	                    FileOpenFlags flags);
	~RateLimitFileHandle() override;

	void Close() override;

	// Returns the inner file handle.
	FileHandle &GetInnerHandle();

private:
	unique_ptr<FileHandle> inner_handle;
};

// A file system wrapper that applies rate limiting to operations.
// Wraps an inner file system and applies rate limits based on the configuration.
class RateLimitFileSystem : public FileSystem {
public:
	// Creates a rate limit file system wrapping the given inner file system and config.
	RateLimitFileSystem(unique_ptr<FileSystem> inner_fs_p, shared_ptr<RateLimitConfig> config_p);

	// Creates a rate limit file system wrapping a new local file system.
	explicit RateLimitFileSystem(shared_ptr<RateLimitConfig> config_p);

	~RateLimitFileSystem() override;

	// Returns the inner file system.
	FileSystem &GetInnerFileSystem() const;

	// ==========================================================================
	// Rate limited operations
	// ==========================================================================

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;

	void Truncate(FileHandle &handle, int64_t new_size) override;

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;

	// ==========================================================================
	// Delegate to inner file system (no rate limiting)
	// ==========================================================================

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	void FileSync(FileHandle &handle) override;

	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;

	bool CanSeek() override;
	bool OnDiskFile(FileHandle &handle) override;

	string GetName() const override;
	string PathSeparator(const string &path) override;

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;
	bool SupportsOpenFileExtended() const override;

	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override;
	bool SupportsListFilesExtended() const override;

private:
	// Applies rate limiting for the specified operation and byte count.
	// If rate limiting is configured for this operation, waits or throws based on mode.
	void ApplyRateLimit(FileSystemOperation operation, idx_t bytes = 1);

	// Extracts the inner file handle from a potentially wrapped handle.
	FileHandle &GetInnerFileHandle(FileHandle &handle);

	unique_ptr<FileSystem> inner_fs;
	shared_ptr<RateLimitConfig> config;
};

} // namespace duckdb
