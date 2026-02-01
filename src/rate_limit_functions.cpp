#include "rate_limit_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "rate_limit_config.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// rate_limit_fs_quota(operation, value, mode)
//===--------------------------------------------------------------------===//

void RateLimitFsQuotaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto config = RateLimitConfig::GetOrCreate(context);

	auto &operation_vector = args.data[0];
	auto &value_vector = args.data[1];
	auto &mode_vector = args.data[2];

	TernaryExecutor::Execute<string_t, int64_t, string_t, string_t>(
	    operation_vector, value_vector, mode_vector, result, args.size(),
	    [&](string_t operation, int64_t value, string_t mode) {
		    if (value < 0) {
			    throw InvalidInputException("Quota value must be non-negative, got %lld", value);
		    }
		    auto mode_enum = ParseRateLimitMode(mode.GetString());
		    config->SetQuota(operation.GetString(), static_cast<idx_t>(value), mode_enum);
		    return operation;
	    });
}

//===--------------------------------------------------------------------===//
// rate_limit_fs_burst(operation, value)
//===--------------------------------------------------------------------===//

void RateLimitFsBurstFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto config = RateLimitConfig::GetOrCreate(context);

	auto &operation_vector = args.data[0];
	auto &value_vector = args.data[1];

	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    operation_vector, value_vector, result, args.size(), [&](string_t operation, int64_t value) {
		    if (value < 0) {
			    throw InvalidInputException("Burst value must be non-negative, got %lld", value);
		    }
		    config->SetBurst(operation.GetString(), static_cast<idx_t>(value));
		    return operation;
	    });
}

//===--------------------------------------------------------------------===//
// rate_limit_fs_clear(operation)
//===--------------------------------------------------------------------===//

void RateLimitFsClearFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto config = RateLimitConfig::GetOrCreate(context);

	auto &operation_vector = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(operation_vector, result, args.size(), [&](string_t operation) {
		string op_str = operation.GetString();
		if (op_str == "*") {
			config->ClearAll();
			return StringVector::AddString(result, "all");
		}
		config->ClearConfig(op_str);
		return operation;
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

		output.SetValue(0, count, Value(config.operation));
		output.SetValue(1, count, Value::BIGINT(static_cast<int64_t>(config.quota)));
		output.SetValue(2, count, Value(RateLimitModeToString(config.mode)));
		output.SetValue(3, count, Value::BIGINT(static_cast<int64_t>(config.burst)));

		state.current_idx++;
		count++;
	}

	output.SetCardinality(count);
}

} // namespace

ScalarFunction GetRateLimitFsQuotaFunction() {
	return ScalarFunction("rate_limit_fs_quota",
	                      {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::BIGINT},
	                       LogicalType {LogicalTypeId::VARCHAR}},
	                      LogicalType {LogicalTypeId::VARCHAR}, RateLimitFsQuotaFunction);
}

ScalarFunction GetRateLimitFsBurstFunction() {
	return ScalarFunction("rate_limit_fs_burst",
	                      {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::BIGINT}},
	                      LogicalType {LogicalTypeId::VARCHAR}, RateLimitFsBurstFunction);
}

ScalarFunction GetRateLimitFsClearFunction() {
	return ScalarFunction("rate_limit_fs_clear", {LogicalType {LogicalTypeId::VARCHAR}},
	                      LogicalType {LogicalTypeId::VARCHAR}, RateLimitFsClearFunction);
}

TableFunction GetRateLimitFsConfigsFunction() {
	TableFunction func("rate_limit_fs_configs", {}, RateLimitConfigsFunction, RateLimitConfigsBind,
	                   RateLimitConfigsInit);
	return func;
}

} // namespace duckdb
