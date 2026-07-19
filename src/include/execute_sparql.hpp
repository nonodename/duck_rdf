#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void RegisterExecuteSparql(ExtensionLoader &loader);

} // namespace duckdb
