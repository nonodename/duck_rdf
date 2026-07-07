#include "include/table_filter_eval.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include <cstring>

using namespace duckdb;

static bool CompareBytes(ExpressionType comparison_type, const char *data, idx_t len, const string &constant) {
	int cmp;
	idx_t min_len = std::min<idx_t>(len, constant.size());
	cmp = min_len == 0 ? 0 : std::memcmp(data, constant.data(), min_len);
	if (cmp == 0) {
		if (len < constant.size()) {
			cmp = -1;
		} else if (len > constant.size()) {
			cmp = 1;
		}
	}
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return cmp == 0;
	case ExpressionType::COMPARE_NOTEQUAL:
		return cmp != 0;
	case ExpressionType::COMPARE_LESSTHAN:
		return cmp < 0;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return cmp <= 0;
	case ExpressionType::COMPARE_GREATERTHAN:
		return cmp > 0;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return cmp >= 0;
	default:
		// Unsupported comparison type for raw-byte pushdown - don't reject.
		return true;
	}
}

bool PassesFilter(const TableFilter *filter, const char *data, idx_t len, bool is_null) {
	if (!filter) {
		return true;
	}
	switch (filter->filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		if (is_null) {
			// NULL never satisfies a constant comparison.
			return false;
		}
		auto &constant_filter = filter->Cast<ConstantFilter>();
		if (constant_filter.constant.IsNull() || constant_filter.constant.type().id() != LogicalTypeId::VARCHAR) {
			return true;
		}
		return CompareBytes(constant_filter.comparison_type, data, len, StringValue::Get(constant_filter.constant));
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter->Cast<ConjunctionAndFilter>();
		for (auto &child : and_filter.child_filters) {
			if (!PassesFilter(child.get(), data, len, is_null)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::IS_NULL:
		return is_null;
	case TableFilterType::IS_NOT_NULL:
		return !is_null;
	default:
		// Unsupported filter type - safe to pass through (see header comment).
		return true;
	}
}
