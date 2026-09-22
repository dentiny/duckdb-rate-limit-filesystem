#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterRateLimitFunctions(ExtensionLoader &loader);

} // namespace duckdb
