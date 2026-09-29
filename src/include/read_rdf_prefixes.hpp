#pragma once

#include "duckdb.hpp"
#define PREFIXES_FUNCTION_NAME           "read_rdf_prefixes"

namespace duckdb {

void RegisterReadRDFPrefixes(ExtensionLoader &loader);

} // namespace duckdb
