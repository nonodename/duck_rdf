#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <string>

namespace duckdb {

void RegisterSparqlToSql(ExtensionLoader &loader);

// Loads mapping_path, parses sparql_text, and translates it into a standalone
// SQL query for the "duckdb" dialect. Shared by the sparql_to_sql scalar
// function, execute_sparql's bind_replace, and the SPARQL parser extension so
// all three raise identical errors. context may be null (as when called from
// the parser extension, which has no ClientContext) - the mapping-existence
// pre-check and the best-effort type catalog are simply skipped in that case.
std::string TranslateSparqlToSql(ClientContext *context, const std::string &sparql_text,
                                 const std::string &mapping_path);

} // namespace duckdb
