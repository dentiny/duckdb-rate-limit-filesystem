#include "rate_limit_function_registration.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "rate_limit_functions.hpp"

namespace duckdb {

namespace {

template <class CREATE_INFO, class FUNCTION>
void RegisterFunction(ExtensionLoader &loader, FUNCTION function, vector<string> parameter_names, string description,
                      vector<string> examples, vector<string> categories) {
	CREATE_INFO info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;

	FunctionDescription function_description;
	function_description.parameter_names = std::move(parameter_names);
	function_description.description = std::move(description);
	function_description.examples = std::move(examples);
	function_description.categories = std::move(categories);
	info.descriptions.push_back(std::move(function_description));

	loader.RegisterFunction(std::move(info));
}

void RegisterScalarFunction(ExtensionLoader &loader, ScalarFunction function, vector<string> parameter_names,
                            string description, vector<string> examples, vector<string> categories) {
	RegisterFunction<CreateScalarFunctionInfo>(loader, std::move(function), std::move(parameter_names),
	                                           std::move(description), std::move(examples), std::move(categories));
}

void RegisterTableFunction(ExtensionLoader &loader, TableFunction function, vector<string> parameter_names,
                           string description, vector<string> examples, vector<string> categories) {
	RegisterFunction<CreateTableFunctionInfo>(loader, std::move(function), std::move(parameter_names),
	                                          std::move(description), std::move(examples), std::move(categories));
}

} // namespace

void RegisterRateLimitFunctions(ExtensionLoader &loader) {
	RegisterScalarFunction(
	    loader, GetRateLimitFsQuotaFunction(),
	    /*parameter_names=*/ {"filesystem_name", "operation", "value", "mode"},
	    /*description=*/
	    "Sets the sustained rate limit for a filesystem operation in blocking or non-blocking mode.",
	    /*examples=*/
	    {"SELECT rate_limit_fs_quota('RateLimitFileSystem - LocalFileSystem', 'read', 1048576, 'blocking');"},
	    /*categories=*/ {"filesystem", "rate_limiting"});
	RegisterScalarFunction(
	    loader, GetRateLimitFsBurstFunction(),
	    /*parameter_names=*/ {"filesystem_name", "operation", "value"},
	    /*description=*/"Sets the burst capacity for read or write operations on a rate-limited filesystem.",
	    /*examples=*/
	    {"SELECT rate_limit_fs_burst('RateLimitFileSystem - LocalFileSystem', 'read', 10485760);"},
	    /*categories=*/ {"filesystem", "rate_limiting"});
	RegisterScalarFunction(
	    loader, GetRateLimitFsMaxRequestsFunction(),
	    /*parameter_names=*/ {"filesystem_name", "operation", "value"},
	    /*description=*/
	    "Sets the maximum number of concurrent requests for a filesystem operation, or -1 for unlimited.",
	    /*examples=*/
	    {"SELECT rate_limit_fs_max_requests('RateLimitFileSystem - LocalFileSystem', 'read', 10);"},
	    /*categories=*/ {"filesystem", "rate_limiting", "concurrency"});
	RegisterScalarFunction(
	    loader, GetRateLimitFsClearFunction(),
	    /*parameter_names=*/ {"filesystem_name", "operation"},
	    /*description=*/"Clears one or more rate-limit settings; '*' selects every filesystem or operation.",
	    /*examples=*/ {"SELECT rate_limit_fs_clear('RateLimitFileSystem - LocalFileSystem', 'read');"},
	    /*categories=*/ {"filesystem", "rate_limiting"});
	RegisterTableFunction(
	    loader, GetRateLimitFsConfigsFunction(),
	    /*parameter_names=*/ {},
	    /*description=*/
	    "Returns all configured filesystem operation quotas, modes, burst limits, and concurrency caps.",
	    /*examples=*/ {"SELECT * FROM rate_limit_fs_configs();"},
	    /*categories=*/ {"filesystem", "rate_limiting"});
	RegisterTableFunction(
	    loader, GetRateLimitFsListFilesystemsFunction(),
	    /*parameter_names=*/ {},
	    /*description=*/"Lists the filesystem implementations registered with DuckDB's virtual filesystem.",
	    /*examples=*/ {"SELECT * FROM rate_limit_fs_list_filesystems();"},
	    /*categories=*/ {"filesystem"});
	RegisterScalarFunction(
	    loader, GetRateLimitFsWrapFunction(),
	    /*parameter_names=*/ {"filesystem_name"},
	    /*description=*/"Wraps a registered filesystem with rate limiting and registers the wrapped filesystem.",
	    /*examples=*/ {"SELECT rate_limit_fs_wrap('LocalFileSystem');"},
	    /*categories=*/ {"filesystem", "rate_limiting"});
}

} // namespace duckdb
