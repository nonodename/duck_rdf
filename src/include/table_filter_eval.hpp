#ifndef TABLE_FILTER_EVAL_H
#define TABLE_FILTER_EVAL_H

#include "duckdb.hpp"
#include "duckdb/planner/table_filter.hpp"

// Evaluates a single-column DuckDB TableFilter against a raw byte value without
// requiring it to be materialized as a duckdb::Value or std::string first.
//
// Returns false only if `filter` definitely rejects this value. Returns true if
// the value satisfies the filter, if `filter` is null, or if the filter is of a
// type this function doesn't understand. The "unsupported -> true" default is
// safe here: DuckDB only fully removes the corresponding WHERE clause from the
// plan (trusting the table function to enforce it) for ConstantFilter,
// ConjunctionAndFilter combinations of those, and IsNull/IsNotNull filters -
// exactly the cases this function implements. Anything else it might push down
// (OR-clauses, IN-lists, LIKE wildcard bounds) arrives wrapped so that the
// original Filter operator stays in the plan, so passing those through here is
// only ever a missed optimization, never a correctness problem.
bool PassesFilter(const duckdb::TableFilter *filter, const char *data, duckdb::idx_t len, bool is_null);

#endif // TABLE_FILTER_EVAL_H
