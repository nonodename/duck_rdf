#pragma once

#include "duckdb.hpp"

#define PIVOT_FUNCTION_NAME "pivot_rdf"

namespace duckdb {

void RegisterPivotRDF(ExtensionLoader &loader);

} // namespace duckdb
