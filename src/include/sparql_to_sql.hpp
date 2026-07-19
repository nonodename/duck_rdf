#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void RegisterSparqlToSql(ExtensionLoader &loader);

} // namespace duckdb
