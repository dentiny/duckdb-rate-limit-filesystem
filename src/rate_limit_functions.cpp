#include "rate_limit_functions.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "file_system_operation.hpp"
#include "rate_limit_config.hpp"
#include "rate_limit_file_system.hpp"

namespace duckdb {

namespace {

// Helper to get the VirtualFileSystem from context
FileSystem &GetVirtualFileSystem(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &opener_fs = db.GetFileSystem().Cast<OpenerFileSystem>();
	return opener_fs.GetFileSystem();
}

// Helper to validate that a filesystem exists
void ValidateFilesystemExists(ClientContext &context, const string &fs_name) {
	auto &vfs = GetVirtualFileSystem(context);
	auto filesystems = vfs.ListSubSystems();
	for (const auto &name : filesystems) {
		if (name == fs_name) {
			return;
		}
	}
	throw InvalidInputException(
	    "Filesystem '%s' not found. Use rate_limit_fs_list_filesystems() to see available filesystems.", fs_name);
}

//===--------------------------------------------------------------------===//
// rate_limit_fs_quota(filesystem_name, operation, value, mode)
//===--------------------------------------------------------------------===//

void RateLimitFsQuotaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.size() == 1);
	auto &context = state.GetContext();
	auto config = RateLimitConfig::GetOrCreate(context);

	auto fs_str = args.data[0].GetValue(0).ToString();
	auto op_str = args.data[1].GetValue(0).ToString();
	auto value = args.data[2].GetValue(0).GetValue<int64_t>();
	auto mode_str = args.data[3].GetValue(0).ToString();

	// Extract and validate input.
	ValidateFilesystemExists(context, fs_str);
	if (value < 0) {
		throw InvalidInputException("Quota value must be non-negative, got %lld", value);
	}
	const auto op_enum = ParseFileSystemOperation(op_str);
	const auto mode_enum = ParseRateLimitMode(mode_str);

	config->SetQuota(fs_str, op_enum, static_cast<idx_t>(value), mode_enum);
	result.SetValue(0, Value::BOOLEAN(true));
}

//===--------------------------------------------------------------------===//
// rate_limit_fs_burst(filesystem_name, operation, value)
//===--------------------------------------------------------------------===//

void RateLimitFsBurstFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.size() == 1);
	auto &context = state.GetContext();
	auto config = RateLimitConfig::GetOrCreate(context);

	auto fs_str = args.data[0].GetValue(0).ToString();
	auto op_str = args.data[1].GetValue(0).ToString();
	auto value = args.data[2].GetValue(0).GetValue<int64_t>();

	// Extract and validate input.
	ValidateFilesystemExists(context, fs_str);
	if (value < 0) {
		throw InvalidInputException("Burst value must be non-negative, got %lld", value);
	}
	auto op_enum = ParseFileSystemOperation(op_str);

	config->SetBurst(fs_str, op_enum, static_cast<idx_t>(value));
	result.SetValue(0, Value::BOOLEAN(true));
}

//===--------------------------------------------------------------------===//
// rate_limit_fs_clear(filesystem_name, operation)
// Pass '*' as operation to clear all configs for a filesystem.
// Pass '*' as filesystem_name to clear all configs.
//===--------------------------------------------------------------------===//

void RateLimitFsClearFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.size() == 1);
	auto &context = state.GetContext();
	auto config = RateLimitConfig::GetOrCreate(context);

	auto fs_str = args.data[0].GetValue(0).ToString();
	auto op_str = args.data[1].GetValue(0).ToString();

	if (fs_str == "*") {
		config->ClearAll();
		result.SetValue(0, Value::BOOLEAN(true));
		return;
	}

	if (op_str == "*") {
		config->ClearFilesystem(fs_str);
		result.SetValue(0, Value::BOOLEAN(true));
		return;
	}

	auto op_enum = ParseFileSystemOperation(op_str);
	config->ClearConfig(fs_str, op_enum);

	result.SetValue(0, Value::BOOLEAN(true));
}

//===--------------------------------------------------------------------===//
// rate_limit_fs_configs() - Table Function
//===--------------------------------------------------------------------===//

struct RateLimitConfigsData : public GlobalTableFunctionState {
	vector<OperationConfig> configs;
	idx_t current_idx;

	RateLimitConfigsData() : current_idx(0) {
	}
};

unique_ptr<FunctionData> RateLimitConfigsBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	return_types.reserve(5);
	names.reserve(5);

	names.emplace_back("filesystem");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});

	names.emplace_back("operation");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});

	names.emplace_back("quota");
	return_types.emplace_back(LogicalType {LogicalTypeId::BIGINT});

	names.emplace_back("mode");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});

	names.emplace_back("burst");
	return_types.emplace_back(LogicalType {LogicalTypeId::BIGINT});

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> RateLimitConfigsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<RateLimitConfigsData>();
	auto config = RateLimitConfig::Get(context);
	if (config) {
		result->configs = config->GetAllConfigs();
	}
	return std::move(result);
}

void RateLimitConfigsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RateLimitConfigsData>();

	idx_t count = 0;
	while (state.current_idx < state.configs.size() && count < STANDARD_VECTOR_SIZE) {
		auto &config = state.configs[state.current_idx];

		output.SetValue(0, count, Value(config.filesystem_name));
		output.SetValue(1, count, Value(FileSystemOperationToString(config.operation)));
		output.SetValue(2, count, Value::BIGINT(static_cast<int64_t>(config.quota)));
		output.SetValue(3, count, Value(RateLimitModeToString(config.mode)));
		output.SetValue(4, count, Value::BIGINT(static_cast<int64_t>(config.burst)));

		state.current_idx++;
		count++;
	}

	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// rate_limit_fs_list_filesystems() - Table Function
//===--------------------------------------------------------------------===//

struct ListFilesystemsData : public GlobalTableFunctionState {
	vector<string> filesystems;
	idx_t current_idx;

	ListFilesystemsData() : current_idx(0) {
	}
};

unique_ptr<FunctionData> ListFilesystemsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> ListFilesystemsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<ListFilesystemsData>();
	auto &vfs = GetVirtualFileSystem(context);
	result->filesystems = vfs.ListSubSystems();
	std::sort(result->filesystems.begin(), result->filesystems.end());
	return std::move(result);
}

void ListFilesystemsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<ListFilesystemsData>();

	idx_t count = 0;
	while (state.current_idx < state.filesystems.size() && count < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, count, Value(state.filesystems[state.current_idx]));

		state.current_idx++;
		count++;
	}

	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// rate_limit_fs_wrap(filesystem_name)
//===--------------------------------------------------------------------===//

void RateLimitFsWrapFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &vfs = GetVirtualFileSystem(context);
	auto config = RateLimitConfig::GetOrCreate(context);

	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [&](string_t fs_name) {
		string fs_str = fs_name.GetString();

		// Extract the filesystem from the VFS
		auto extracted_fs = vfs.ExtractSubSystem(fs_str);
		if (!extracted_fs) {
			throw InvalidInputException("Filesystem '%s' not found or cannot be extracted. "
			                            "Use rate_limit_fs_list_filesystems() to see available filesystems.",
			                            fs_str);
		}

		// Wrap it with RateLimitFileSystem
		auto wrapped_fs = make_uniq<RateLimitFileSystem>(std::move(extracted_fs), config);
		string wrapped_name = wrapped_fs->GetName();

		// Register the wrapped filesystem
		vfs.RegisterSubSystem(std::move(wrapped_fs));

		// Log filesystem registration
		auto db = config->GetDatabaseInstance();
		DUCKDB_LOG_DEBUG(*db, StringUtil::Format("Wrap filesystem %s with rate limit filesystem (registered as %s).",
		                                         fs_str, wrapped_name));

		return true;
	});
}

} // namespace

ScalarFunction GetRateLimitFsQuotaFunction() {
	return ScalarFunction("rate_limit_fs_quota",
	                      {/*filesystem_name=*/LogicalType {LogicalTypeId::VARCHAR},
	                       /*operation=*/LogicalType {LogicalTypeId::VARCHAR},
	                       /*value=*/LogicalType {LogicalTypeId::BIGINT},
	                       /*mode=*/LogicalType {LogicalTypeId::VARCHAR}},
	                      LogicalType {LogicalTypeId::BOOLEAN}, RateLimitFsQuotaFunction);
}

ScalarFunction GetRateLimitFsBurstFunction() {
	return ScalarFunction("rate_limit_fs_burst",
	                      {/*filesystem_name=*/LogicalType {LogicalTypeId::VARCHAR},
	                       /*operation=*/LogicalType {LogicalTypeId::VARCHAR},
	                       /*value=*/LogicalType {LogicalTypeId::BIGINT}},
	                      LogicalType {LogicalTypeId::BOOLEAN}, RateLimitFsBurstFunction);
}

ScalarFunction GetRateLimitFsClearFunction() {
	return ScalarFunction("rate_limit_fs_clear",
	                      {/*filesystem_name=*/LogicalType {LogicalTypeId::VARCHAR},
	                       /*operation=*/LogicalType {LogicalTypeId::VARCHAR}},
	                      LogicalType {LogicalTypeId::BOOLEAN}, RateLimitFsClearFunction);
}

TableFunction GetRateLimitFsConfigsFunction() {
	TableFunction func("rate_limit_fs_configs", {}, RateLimitConfigsFunction, RateLimitConfigsBind,
	                   RateLimitConfigsInit);
	return func;
}

TableFunction GetRateLimitFsListFilesystemsFunction() {
	TableFunction func("rate_limit_fs_list_filesystems", {}, ListFilesystemsFunction, ListFilesystemsBind,
	                   ListFilesystemsInit);
	return func;
}

ScalarFunction GetRateLimitFsWrapFunction() {
	return ScalarFunction("rate_limit_fs_wrap", {/*filesystem_name=*/LogicalType {LogicalTypeId::VARCHAR}},
	                      LogicalType {LogicalTypeId::BOOLEAN}, RateLimitFsWrapFunction);
}

} // namespace duckdb
