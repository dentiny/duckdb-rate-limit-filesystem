#include "rate_limit_mode.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

RateLimitMode ParseRateLimitMode(const string &mode_str) {
	auto mode_lower = StringUtil::Lower(mode_str);

	if (mode_lower == "blocking" || mode_lower == "block") {
		return RateLimitMode::BLOCKING;
	}
	if (mode_lower == "non_blocking" || mode_lower == "non-blocking" || mode_lower == "nonblocking") {
		return RateLimitMode::NON_BLOCKING;
	}

	throw InvalidInputException("Invalid rate limit mode '%s'. Use 'blocking' or 'non_blocking'", mode_str);
}

string RateLimitModeToString(RateLimitMode mode) {
	switch (mode) {
	case RateLimitMode::BLOCKING:
		return "blocking";
	case RateLimitMode::NON_BLOCKING:
		return "non_blocking";
	default:
		return "unknown";
	}
}

} // namespace duckdb
