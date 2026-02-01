#define DUCKDB_EXTENSION_MAIN

#include "rate_limit_filesystem_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void RateLimitFilesystemScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "RateLimitFilesystem " + name.GetString() + " 🐥");
	});
}

inline void RateLimitFilesystemOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "RateLimitFilesystem " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto rate_limit_filesystem_scalar_function = ScalarFunction("rate_limit_filesystem", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RateLimitFilesystemScalarFun);
	loader.RegisterFunction(rate_limit_filesystem_scalar_function);

	// Register another scalar function
	auto rate_limit_filesystem_openssl_version_scalar_function = ScalarFunction("rate_limit_filesystem_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, RateLimitFilesystemOpenSSLVersionScalarFun);
	loader.RegisterFunction(rate_limit_filesystem_openssl_version_scalar_function);
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
