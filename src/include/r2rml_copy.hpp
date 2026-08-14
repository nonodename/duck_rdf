#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <r2rml/R2RMLMapping.h>
#include <r2rml/SQLConnection.h>
#include <memory>
#include <string>

namespace duckdb {

void RegisterR2RMLCopy(ExtensionLoader &loader);

// Parses an R2RML (Turtle) or YARRRML (YAML) mapping file, dispatching on the
// file extension (yarrrml::YARRRMLParser::hasYarrrmlExtension). Shared with
// sparql_to_sql.cpp so mapping-format dispatch lives in exactly one place.
r2rml::R2RMLMapping ParseR2RMLOrYarrrmlMapping(const std::string &path, bool ignore_non_fatal_errors = true);

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
