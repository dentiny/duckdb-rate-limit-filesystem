#define DUCKDB_EXTENSION_MAIN

#include "rate_limit_filesystem_extension.hpp"
#include "fake_filesystem.hpp"
#include "rate_limit_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

namespace {
void LoadInternal(ExtensionLoader &loader) {
	// Register rate limit configuration functions
	loader.RegisterFunction(GetRateLimitFsQuotaFunction());
	loader.RegisterFunction(GetRateLimitFsBurstFunction());
	loader.RegisterFunction(GetRateLimitFsClearFunction());
	loader.RegisterFunction(GetRateLimitFsConfigsFunction());

	// Register filesystem management functions
	loader.RegisterFunction(GetRateLimitFsListFilesystemsFunction());
	loader.RegisterFunction(GetRateLimitFsWrapFunction());

	// TODO(hjiang): Register a fake filesystem at extension load for testing purpose. This is not ideal since
	// additional necessary instance is shipped in the extension. Local filesystem is not viable because it's not
	// registered in virtual filesystem. A better approach is find another filesystem not in httpfs extension.
	auto &db = loader.GetDatabaseInstance();
	auto &opener_fs = db.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_fs.GetFileSystem();
	vfs.RegisterSubSystem(make_uniq<RateLimitFsFakeFileSystem>());
}
} // namespace

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
