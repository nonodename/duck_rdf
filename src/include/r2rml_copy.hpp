#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void RegisterR2RMLCopy(ExtensionLoader &loader);

} // namespace duckdb
