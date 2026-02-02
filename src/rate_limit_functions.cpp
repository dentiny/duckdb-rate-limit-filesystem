#include "rate_limit_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "file_system_operation.hpp"
#include "rate_limit_config.hpp"
#include "rate_limit_file_system.hpp"

namespace duckdb {

namespace {

// Helper to validate that a filesystem exists
void ValidateFilesystemExists(ClientContext &context, const string &fs_name) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto filesystems = fs.ListSubSystems();
	bool found = false;
	for (const auto &name : filesystems) {
		if (name == fs_name) {
			found = true;
			break;
		}
	}
	if (!found) {
		throw InvalidInputException(
		    "Filesystem '%s' not found. Use rate_limit_fs_list_filesystems() to see available filesystems.", fs_name);
	}
}

//===--------------------------------------------------------------------===//
// rate_limit_fs_quota(filesystem_name, operation, value, mode)
//===--------------------------------------------------------------------===//

void RateLimitFsQuotaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto config = RateLimitConfig::GetOrCreate(context);

	auto &fs_name_vector = args.data[0];
	auto &operation_vector = args.data[1];
	auto &value_vector = args.data[2];
	auto &mode_vector = args.data[3];

	UnifiedVectorFormat fs_name_data, operation_data, value_data, mode_data;
	fs_name_vector.ToUnifiedFormat(args.size(), fs_name_data);
	operation_vector.ToUnifiedFormat(args.size(), operation_data);
	value_vector.ToUnifiedFormat(args.size(), value_data);
	mode_vector.ToUnifiedFormat(args.size(), mode_data);

	auto fs_names = UnifiedVectorFormat::GetData<string_t>(fs_name_data);
	auto operations = UnifiedVectorFormat::GetData<string_t>(operation_data);
	auto values = UnifiedVectorFormat::GetData<int64_t>(value_data);
	auto modes = UnifiedVectorFormat::GetData<string_t>(mode_data);

	for (idx_t i = 0; i < args.size(); i++) {
		auto fs_idx = fs_name_data.sel->get_index(i);
		auto op_idx = operation_data.sel->get_index(i);
		auto val_idx = value_data.sel->get_index(i);
		auto mode_idx = mode_data.sel->get_index(i);

		string fs_str = fs_names[fs_idx].GetString();
		ValidateFilesystemExists(context, fs_str);

		int64_t value = values[val_idx];
		if (value < 0) {
			throw InvalidInputException("Quota value must be non-negative, got %lld", value);
		}

		auto op_enum = ParseFileSystemOperation(operations[op_idx].GetString());
		auto mode_enum = ParseRateLimitMode(modes[mode_idx].GetString());
		config->SetQuota(fs_str, op_enum, static_cast<idx_t>(value), mode_enum);

		result.SetValue(i, Value(fs_str));
	}
}

//===--------------------------------------------------------------------===//
// rate_limit_fs_burst(filesystem_name, operation, value)
//===--------------------------------------------------------------------===//

void RateLimitFsBurstFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto config = RateLimitConfig::GetOrCreate(context);

	auto &fs_name_vector = args.data[0];
	auto &operation_vector = args.data[1];
	auto &value_vector = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, int64_t, string_t>(
	    fs_name_vector, operation_vector, value_vector, result, args.size(),
	    [&](string_t fs_name, string_t operation, int64_t value) {
		    string fs_str = fs_name.GetString();
		    ValidateFilesystemExists(context, fs_str);
		    if (value < 0) {
			    throw InvalidInputException("Burst value must be non-negative, got %lld", value);
		    }
		    auto op_enum = ParseFileSystemOperation(operation.GetString());
		    config->SetBurst(fs_str, op_enum, static_cast<idx_t>(value));
		    return fs_name;
	    });
}

//===--------------------------------------------------------------------===//
// rate_limit_fs_clear(filesystem_name, operation)
// Pass '*' as operation to clear all configs for a filesystem.
// Pass '*' as filesystem_name to clear all configs.
//===--------------------------------------------------------------------===//

void RateLimitFsClearFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto config = RateLimitConfig::GetOrCreate(context);

	auto &fs_name_vector = args.data[0];
	auto &operation_vector = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, string_t>(fs_name_vector, operation_vector, result, args.size(),
	                                                      [&](string_t fs_name, string_t operation) {
		                                                      string fs_str = fs_name.GetString();
		                                                      string op_str = operation.GetString();

		                                                      if (fs_str == "*") {
			                                                      config->ClearAll();
			                                                      return StringVector::AddString(result, "all");
		                                                      }

		                                                      if (op_str == "*") {
			                                                      config->ClearFilesystem(fs_str);
			                                                      return fs_name;
		                                                      }

		                                                      auto op_enum = ParseFileSystemOperation(op_str);
		                                                      config->ClearConfig(fs_str, op_enum);
		                                                      return fs_name;
	                                                      });
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
	auto &fs = FileSystem::GetFileSystem(context);
	result->filesystems = fs.ListSubSystems();
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
	auto &fs = FileSystem::GetFileSystem(context);
	auto config = RateLimitConfig::GetOrCreate(context);

	auto &name_vector = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t fs_name) {
		string name = fs_name.GetString();

		// Extract the filesystem from the virtual filesystem
		auto inner_fs = fs.ExtractSubSystem(name);
		if (!inner_fs) {
			throw InvalidInputException("Filesystem '%s' not found or is disabled", name);
		}

		// Create the wrapped filesystem
		auto wrapped_fs = make_uniq<RateLimitFileSystem>(std::move(inner_fs), config, name);
		string new_name = wrapped_fs->GetName();

		// Register the wrapped filesystem
		fs.RegisterSubSystem(std::move(wrapped_fs));

		return StringVector::AddString(result, new_name);
	});
}

} // namespace

ScalarFunction GetRateLimitFsQuotaFunction() {
	return ScalarFunction("rate_limit_fs_quota",
	                      {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR},
	                       LogicalType {LogicalTypeId::BIGINT}, LogicalType {LogicalTypeId::VARCHAR}},
	                      LogicalType {LogicalTypeId::VARCHAR}, RateLimitFsQuotaFunction);
}

ScalarFunction GetRateLimitFsBurstFunction() {
	return ScalarFunction("rate_limit_fs_burst",
	                      {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR},
	                       LogicalType {LogicalTypeId::BIGINT}},
	                      LogicalType {LogicalTypeId::VARCHAR}, RateLimitFsBurstFunction);
}

ScalarFunction GetRateLimitFsClearFunction() {
	return ScalarFunction("rate_limit_fs_clear",
	                      {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR}},
	                      LogicalType {LogicalTypeId::VARCHAR}, RateLimitFsClearFunction);
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
	return ScalarFunction("rate_limit_fs_wrap", {LogicalType {LogicalTypeId::VARCHAR}},
	                      LogicalType {LogicalTypeId::VARCHAR}, RateLimitFsWrapFunction);
}

} // namespace duckdb
