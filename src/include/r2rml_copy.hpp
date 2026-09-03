#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <r2rml/R2RMLMapping.h>
#include <r2rml/SQLConnection.h>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

void RegisterR2RMLCopy(ExtensionLoader &loader);

// Resolves `path_or_glob` - a literal path or a glob pattern such as
// "mappings/*.ttl" - to a sorted list of matching file paths. Resolution is
// always against the local filesystem: sql2rdf's parsers read mapping files
// directly (not through DuckDB's virtual FileSystem), and this also lets it
// be called from contexts with no ClientContext at all, such as the SPARQL
// parser_override hot path in sparql_parser.cpp. Returns an empty vector if
// nothing matches.
std::vector<std::string> ResolveMappingFiles(const std::string &path_or_glob);

// Parses the paths passed as a vector as 
// R2RML (Turtle) and/or YARRRML (YAML) mapping file(s), dispatching each file
// on its extension (yarrrml::YARRRMLParser::hasYarrrmlExtension) and merging
// multiple matches via sql2rdf's MappingParser::parseMultiple. Shared with
// sparql_to_sql.cpp so mapping-format dispatch lives in exactly one place.
// Throws std::runtime_error if no file matches or if parsing fails.
r2rml::R2RMLMapping ParseR2RMLOrYarrrmlMapping(std::vector<std::string> paths, const std::string original_path, bool ignoreNonFatalErrors);

// SQLConnection backed by the live DuckDB instance via a fresh Connection.
// Used for full R2RML mode where processDatabase() runs the mapping's SQL
// queries, and shared with sparql_to_sql.cpp's type-catalog loading
// (sql2rdf::loadTypeCatalog also just needs an r2rml::SQLConnection to issue
// information_schema/DESCRIBE queries against).
class ClientContextSQLConnection : public r2rml::SQLConnection {
public:
	ClientContextSQLConnection(ClientContext &ctx, bool ignore_case) : context_(ctx), ignore_case_(ignore_case) {
	}

	std::unique_ptr<r2rml::SQLResultSet> execute(const std::string &sql) override;

	std::string getDefaultSchema() override {
		return "main";
	}

private:
	ClientContext &context_;
	bool ignore_case_;
};

} // namespace duckdb
