#include "include/table_filter_eval.hpp"
#include "duckdb/common/types/value.hpp"

using namespace duckdb;

CompiledColumnFilter CompileColumnFilter(ClientContext &context, optional_ptr<const TableFilter> filter) {
	CompiledColumnFilter result;
	if (!filter) {
		return result;
	}
	if (filter->filter_type == TableFilterType::EXPRESSION_FILTER) {
		result.filter = &filter->Cast<ExpressionFilter>();
	} else {
		result.owned_filter = ExpressionFilter::FromTableFilter(*filter, LogicalType::VARCHAR);
		result.filter = result.owned_filter.get();
	}
	result.context = &context;
	return result;
}

bool PassesFilter(const CompiledColumnFilter &filter, const char *data, idx_t len, bool is_null) {
	if (!filter.filter) {
		return true;
	}
	Value val = is_null ? Value(LogicalType::VARCHAR) : Value(string(data, len));
	return filter.filter->EvaluateWithConstant(*filter.context, val);
}
