#include "file_system_operation.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

string GetValidOperationsString() {
	return "stat, read, write, list, delete";
}

} // namespace

FileSystemOperation ParseFileSystemOperation(const string &op_str) {
	auto lower = StringUtil::Lower(op_str);

	if (lower == "stat") {
		return FileSystemOperation::STAT;
	}
	if (lower == "read") {
		return FileSystemOperation::READ;
	}
	if (lower == "write") {
		return FileSystemOperation::WRITE;
	}
	if (lower == "list") {
		return FileSystemOperation::LIST;
	}
	if (lower == "delete") {
		return FileSystemOperation::DELETE;
	}

	throw InvalidInputException("Invalid operation '%s'. Valid operations are: %s", op_str, GetValidOperationsString());
}

string FileSystemOperationToString(FileSystemOperation op) {
	switch (op) {
	case FileSystemOperation::NONE:
		return "none";
	case FileSystemOperation::STAT:
		return "stat";
	case FileSystemOperation::READ:
		return "read";
	case FileSystemOperation::WRITE:
		return "write";
	case FileSystemOperation::LIST:
		return "list";
	case FileSystemOperation::DELETE:
		return "delete";
	default:
		throw InternalException("Unknown FileSystemOperation value");
	}
}

} // namespace duckdb
