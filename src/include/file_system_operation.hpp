#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

// Filesystem operations that can be rate limited.
enum class FileSystemOperation : uint8_t {
	// No operation
	NONE,
	// File metadata queries (FileExists, DirectoryExists, GetFileSize, etc.)
	STAT,
	// Reading data from files
	READ,
	// Writing data to files (including Truncate, CreateDirectory, MoveFile)
	WRITE,
	// Listing directory contents (Glob, ListFiles)
	LIST,
	// Deleting files or directories
	DELETE
};

// Converts a string to FileSystemOperation. Throws InvalidInputException on invalid input.
FileSystemOperation ParseFileSystemOperation(const string &op_str);

// Converts FileSystemOperation to string (lowercase).
string FileSystemOperationToString(FileSystemOperation op);

} // namespace duckdb
