#define DUCKDB_EXTENSION_MAIN

#include "rate_limit_filesystem_extension.hpp"
#include "rate_limit_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register rate limit configuration functions
	loader.RegisterFunction(GetRateLimitFsQuotaFunction());
	loader.RegisterFunction(GetRateLimitFsBurstFunction());
	loader.RegisterFunction(GetRateLimitFsClearFunction());
	loader.RegisterFunction(GetRateLimitFsConfigsFunction());
}

void RateLimitFilesystemExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RateLimitFilesystemExtension::Name() {
	return "rate_limit_filesystem";
}

std::string RateLimitFilesystemExtension::Version() const {
#ifdef EXT_VERSION_RATE_LIMIT_FILESYSTEM
	return EXT_VERSION_RATE_LIMIT_FILESYSTEM;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(rate_limit_filesystem, loader) {
	duckdb::LoadInternal(loader);
}
}
