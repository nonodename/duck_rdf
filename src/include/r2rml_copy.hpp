#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <r2rml/R2RMLMapping.h>
#include <string>

namespace duckdb {

void RegisterR2RMLCopy(ExtensionLoader &loader);

// Parses an R2RML (Turtle) or YARRRML (YAML) mapping file, dispatching on the
// file extension (yarrrml::YARRRMLParser::hasYarrrmlExtension). Shared with
// sparql_to_sql.cpp so mapping-format dispatch lives in exactly one place.
r2rml::R2RMLMapping ParseR2RMLOrYarrrmlMapping(const std::string &path, bool ignore_non_fatal_errors = true);

} // namespace duckdb
