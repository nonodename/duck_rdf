#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <string>

namespace duckdb {

void RegisterSparqlToSql(ExtensionLoader &loader);

// Loads mapping_path, parses sparql_text, and translates it into a standalone
// SQL query for the "duckdb" dialect. Shared by the sparql_to_sql scalar
// function and execute_sparql's bind_replace so both raise identical errors.
std::string TranslateSparqlToSql(ClientContext &context, const std::string &sparql_text,
                                 const std::string &mapping_path);

} // namespace duckdb
