// A fake filesystem for rate limit filesystem extension testing purpose.

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

// Forward declaration.
class RateLimitFsFakeFileSystem;

class RateLimitFsFakeFsHandle : public FileHandle {
public:
	RateLimitFsFakeFsHandle(string path, unique_ptr<FileHandle> internal_file_handle_p, RateLimitFsFakeFileSystem &fs);
	~RateLimitFsFakeFsHandle() override = default;
	void Close() override {
		internal_file_handle->Close();
	}

	unique_ptr<FileHandle> internal_file_handle;
};

// WARNING: fake filesystem is used for testing purpose and shouldn't be used in production.
class RateLimitFsFakeFileSystem : public FileSystem {
public:
	RateLimitFsFakeFileSystem();
	bool CanHandleFile(const string &path) override;
	string GetName() const override {
		return "RateLimitFsFakeFileSystem";
	}

	// Delegate to local filesystem.
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t GetFileSize(FileHandle &handle) override;
	void FileSync(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool CanSeek() override;
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	bool OnDiskFile(FileHandle &handle) override;

	// Additional file operations
	void Reset(FileHandle &handle) override;
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override;
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override;
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override;
	string PathSeparator(const string &path) override;

	// Directory operations
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) override;
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener) override;

private:
	unique_ptr<FileSystem> local_filesystem;
};

} // namespace duckdb
