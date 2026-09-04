#ifndef TABLE_FILTER_EVAL_H
#define TABLE_FILTER_EVAL_H

#include "duckdb.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

// A pushed-down TableFilter, normalized to an ExpressionFilter. DuckDB represents
// every pushed-down filter (constant comparisons, AND/OR combinations, IN-lists,
// IS [NOT] NULL, LIKE, ...) as an arbitrary bound expression (ExpressionFilter);
// evaluating one against a single value requires a fresh ExpressionExecutor per
// call (ExpressionFilter::EvaluateWithConstant's own contract - reusing an
// executor across independent single-row probes is not safe), so
// CompileColumnFilter() only amortizes the legacy-filter-to-ExpressionFilter
// conversion once per scanned file/range; PassesFilter() still builds an
// executor per row.
struct CompiledColumnFilter {
	// Only set when a legacy (non-expression) filter had to be converted.
	duckdb::unique_ptr<duckdb::ExpressionFilter> owned_filter;
	duckdb::optional_ptr<const duckdb::ExpressionFilter> filter;
	duckdb::ClientContext *context = nullptr;
};

// Returns a CompiledColumnFilter with no filter set when `filter` is null;
// PassesFilter() treats that as "no filter pushed for this column".
CompiledColumnFilter CompileColumnFilter(duckdb::ClientContext &context,
                                         duckdb::optional_ptr<const duckdb::TableFilter> filter);

// Evaluates `filter` against a raw byte value (avoiding a duckdb::Value/
// std::string round-trip only in the common case of no filter at all). Returns
// true if the value satisfies the filter, or if `filter` has none pushed for
// this column.
bool PassesFilter(const CompiledColumnFilter &filter, const char *data, duckdb::idx_t len, bool is_null);

#endif // TABLE_FILTER_EVAL_H
