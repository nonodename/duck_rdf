#pragma once

#include "duckdb.hpp"
#define PROFILE_FUNCTION_NAME "profile_rdf"
namespace duckdb {

void RegisterProfileRDF(ExtensionLoader &loader);

} // namespace duckdb
