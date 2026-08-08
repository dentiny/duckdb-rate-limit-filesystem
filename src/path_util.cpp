#include "path_util.hpp"

#include "duckdb/common/path.hpp"

namespace duckdb {

// TODO(hjiang): should write a schema parsing function.
string ExtractBucket(const string &path) {
	// Only try to parse as URL if path contains "://"
	if (path.find("://") == string::npos) {
		return "";
	}

	try {
		auto parsed_path = Path::FromString(path);
		return parsed_path.GetAuthority();
	} catch (...) {
		return "";
	}
}

} // namespace duckdb
