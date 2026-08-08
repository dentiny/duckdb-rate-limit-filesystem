#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Extracts the bucket name from a path using DuckDB's Path parser.
string ExtractBucket(const string &path);

} // namespace duckdb
