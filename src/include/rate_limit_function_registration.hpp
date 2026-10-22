#pragma once

namespace duckdb {

class ExtensionLoader;

// Registers the rate-limit SQL functions and their catalog documentation.
void RegisterRateLimitFunctions(ExtensionLoader &loader);

} // namespace duckdb
