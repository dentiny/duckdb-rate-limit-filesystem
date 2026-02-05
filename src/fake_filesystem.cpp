#include "fake_filesystem.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"

#if defined(_WIN32)
#include <windows.h>
// Undefine Windows macros that conflict with DuckDB FileSystem method names
#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory
#endif

namespace duckdb {

namespace {
string GetTemporaryDirectory() {
#if defined(_WIN32)
	char temp_path[MAX_PATH];
	DWORD ret = GetTempPathA(MAX_PATH, temp_path);
	if (ret > 0 && ret < MAX_PATH) {
		// GetTempPath returns path with trailing backslash, remove it
		string result(temp_path);
		if (!result.empty() && (result.back() == '\\' || result.back() == '/')) {
			result.pop_back();
		}
		return result;
	}
	// Fallback to environment variables
	const char *temp = std::getenv("TEMP");
	if (temp != nullptr) {
		return string(temp);
	}
	const char *tmp = std::getenv("TMP");
	if (tmp != nullptr) {
		return string(tmp);
	}
	// Last resort fallback
	return "C:\\Temp";
#else
	return "/tmp";
#endif
}
string GetFakeFileSystemDirectory() {
	auto temp_dir = GetTemporaryDirectory();
	auto local_fs = LocalFileSystem::CreateLocal();
	return local_fs->JoinPath(temp_dir, "fake_rate_limit_fs");
}
} // namespace

RateLimitFsFakeFsHandle::RateLimitFsFakeFsHandle(string path, unique_ptr<FileHandle> internal_file_handle_p,
                                                 RateLimitFsFakeFileSystem &fs)
    : FileHandle(fs, std::move(path), internal_file_handle_p->GetFlags()),
      internal_file_handle(std::move(internal_file_handle_p)) {
}

RateLimitFsFakeFileSystem::RateLimitFsFakeFileSystem() : local_filesystem(FileSystem::CreateLocal()) {
	// Create the fake directory if it doesn't exist
	const auto fake_filesystem_directory = GetFakeFileSystemDirectory();
	if (!local_filesystem->DirectoryExists(fake_filesystem_directory)) {
		local_filesystem->CreateDirectory(fake_filesystem_directory);
	}
}

bool RateLimitFsFakeFileSystem::CanHandleFile(const string &path) {
	return StringUtil::StartsWith(path, GetFakeFileSystemDirectory());
}

unique_ptr<FileHandle> RateLimitFsFakeFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                           optional_ptr<FileOpener> opener) {
	auto file_handle = local_filesystem->OpenFile(path, flags, opener);
	return make_uniq<RateLimitFsFakeFsHandle>(path, std::move(file_handle), *this);
}

void RateLimitFsFakeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	local_filesystem->Read(*local_filesystem_handle, buffer, nr_bytes, location);
}

int64_t RateLimitFsFakeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	return local_filesystem->Read(*local_filesystem_handle, buffer, nr_bytes);
}

void RateLimitFsFakeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	local_filesystem->Write(*local_filesystem_handle, buffer, nr_bytes, location);
}

int64_t RateLimitFsFakeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	return local_filesystem->Write(*local_filesystem_handle, buffer, nr_bytes);
}

int64_t RateLimitFsFakeFileSystem::GetFileSize(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	return local_filesystem->GetFileSize(*local_filesystem_handle);
}

void RateLimitFsFakeFileSystem::FileSync(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	local_filesystem->FileSync(*local_filesystem_handle);
}

void RateLimitFsFakeFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	local_filesystem->Seek(*local_filesystem_handle, location);
}

idx_t RateLimitFsFakeFileSystem::SeekPosition(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	return local_filesystem->SeekPosition(*local_filesystem_handle);
}

bool RateLimitFsFakeFileSystem::CanSeek() {
	return local_filesystem->CanSeek();
}

bool RateLimitFsFakeFileSystem::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	return local_filesystem->Trim(*local_filesystem_handle, offset_bytes, length_bytes);
}

timestamp_t RateLimitFsFakeFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	return local_filesystem->GetLastModifiedTime(*local_filesystem_handle);
}

FileType RateLimitFsFakeFileSystem::GetFileType(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	return local_filesystem->GetFileType(*local_filesystem_handle);
}

void RateLimitFsFakeFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	local_filesystem->Truncate(*local_filesystem_handle, new_size);
}

bool RateLimitFsFakeFileSystem::OnDiskFile(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	return local_filesystem->OnDiskFile(*local_filesystem_handle);
}

void RateLimitFsFakeFileSystem::Reset(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<RateLimitFsFakeFsHandle>().internal_file_handle;
	local_filesystem->Reset(*local_filesystem_handle);
}

bool RateLimitFsFakeFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return local_filesystem->IsPipe(filename, opener);
}

bool RateLimitFsFakeFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	return local_filesystem->TryRemoveFile(filename, opener);
}

void RateLimitFsFakeFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	local_filesystem->MoveFile(source, target, opener);
}

string RateLimitFsFakeFileSystem::PathSeparator(const string &path) {
	return local_filesystem->PathSeparator(path);
}

bool RateLimitFsFakeFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return local_filesystem->DirectoryExists(directory, opener);
}

void RateLimitFsFakeFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	local_filesystem->CreateDirectory(directory, opener);
}

void RateLimitFsFakeFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	local_filesystem->RemoveDirectory(directory, opener);
}

bool RateLimitFsFakeFileSystem::ListFiles(const string &directory,
                                          const std::function<void(const string &, bool)> &callback,
                                          FileOpener *opener) {
	return local_filesystem->ListFiles(directory, callback, opener);
}

bool RateLimitFsFakeFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return local_filesystem->FileExists(filename, opener);
}

void RateLimitFsFakeFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	local_filesystem->RemoveFile(filename, opener);
}

vector<OpenFileInfo> RateLimitFsFakeFileSystem::Glob(const string &path, FileOpener *opener) {
	return local_filesystem->Glob(path, opener);
}

} // namespace duckdb
