#pragma once

namespace duckdb {

class ExtensionLoader;

// Registers enable_sparql_parser(mapping_path)/disable_sparql_parser(), and a
// ParserExtension that lets a raw SPARQL statement run directly (translated
// via the same TranslateSparqlToSql() pipeline used by sparql_to_sql() and
// execute_sparql()) once enabled. Database-wide, not per-connection - see
// SparqlParserState in sparql_parser.cpp.
void RegisterSparqlParser(ExtensionLoader &loader);

} // namespace duckdb
