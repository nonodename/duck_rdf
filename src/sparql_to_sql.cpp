#include "include/sparql_to_sql.hpp"
#include "include/r2rml_copy.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include <r2rml/R2RMLMapping.h>
#include <sparql-parser/ParseError.h>
#include <sparql-parser/Parser.h>
#include <sparql2sql/DuckDbDialect.h>
#include <sparql2sql/Translator.h>
#include <sparql2sql/TranslationError.h>
#include <sparql2sql/TypeCatalog.h>
#include <sql2rdf/TypeCatalogLoader.h>
#include <memory>

namespace duckdb {

static std::string DescribeParseError(const sparql::ParseError &e) {
	return "SPARQL parse error: " + e.message() + " (line " + std::to_string(e.line()) + ", column " +
	       std::to_string(e.column()) + ", near '" + e.nearText() + "')";
}

// Best-effort column-type/constraint catalog for translateQuery(). It backs:
// (a) the native-join-key optimization (an equi-join between two base columns
// of comparable declared type is emitted uncast instead of the
// always-correct-but-slower VARCHAR-cast form), (b) R2RML Section 10.2's
// natural-datatype inference on bare rr:column literals, (c) dropping the "IS
// NOT NULL" guard on a column the DDL declares NOT NULL, and (d) dropping a
// candidate arm's DISTINCT once a declared PRIMARY KEY/UNIQUE constraint
// proves its projected columns already determine the row. R2RML/YARRRML
// mappings carry no SQL type or constraint info of their own, so
// sql2rdf::loadTypeCatalog (sql2rdf v2.1.9+) reads it from three sources:
// every base table's columns + nullability via one information_schema.columns
// sweep, each base table's PRIMARY KEY/UNIQUE constraints via
// information_schema.table_constraints/key_column_usage, and each
// rr:sqlQuery logical table's result columns via DESCRIBE (mapping may be
// nullptr to skip the last one; a view gets no constraint facts, since no
// backend reports constraints for an arbitrary query's result). Any failure
// here must never break translation - fall back to nullptr (today's
// behavior).
static std::unique_ptr<sparql2sql::TypeCatalog> BuildTypeCatalog(ClientContext &context,
                                                                 const r2rml::R2RMLMapping *mapping) {
	auto catalog = make_uniq<sparql2sql::TypeCatalog>();
	try {
		ClientContextSQLConnection conn(context, /*ignore_case=*/false);
		sql2rdf::loadTypeCatalog(conn, mapping, *catalog);
	} catch (...) {
		return nullptr;
	}
	return std::move(catalog);
}

std::string TranslateSparqlToSql(ClientContext *context, const std::string &sparql_text,
                                 const std::string &mapping_path) {
	if (ResolveMappingFiles(mapping_path).empty()) {
		throw IOException("Mapping file not found: " + mapping_path);
	}

	r2rml::R2RMLMapping mapping;
	try {
		auto paths = ResolveMappingFiles(mapping_path);
		mapping = ParseR2RMLOrYarrrmlMapping(paths, mapping_path, true);
	} catch (const std::runtime_error &e) {
		throw InvalidInputException("R2RML/YARRRML mapping parse error: %s", e.what());
	}

	if (!mapping.isValid()) {
		throw InvalidInputException(
		    "Mapping '%s' is not a valid full R2RML mapping. sparql_to_sql() translates a SPARQL query into "
		    "a standalone SQL query, so every TriplesMap in the mapping must declare an rr:logicalTable (or "
		    "YARRRML 'sources') naming the table/view to query. An inside-out-only mapping (one that "
		    "can_call_inside_out() accepts but is_valid_r2rml() rejects) is not sufficient here.",
		    mapping_path.c_str());
	}

	std::unique_ptr<sparql::ast::Query> query;
	try {
		sparql::Parser parser;
		query = parser.parseString(sparql_text);
	} catch (const sparql::ParseError &e) {
		throw InvalidInputException(DescribeParseError(e));
	} catch (const std::exception &e) {
		throw InvalidInputException("SPARQL parse error: %s", e.what());
	}

	std::string sql;
	try {
		sparql2sql::DuckDbDialect dialect;
		auto catalog = context ? BuildTypeCatalog(*context, &mapping) : nullptr;
		sql = sparql2sql::translateQuery(*query, mapping, dialect, catalog.get());
	} catch (const std::exception &e) {
		throw InvalidInputException("SPARQL-to-SQL translation error: %s", e.what());
	}

	return sql;
}

inline void SparqlToSql(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &sparql_vector = args.data[0];
	auto &mapping_vector = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    sparql_vector, mapping_vector, result, args.size(), [&](string_t sparql_text, string_t mapping_path_str) {
		    std::string sql =
		        TranslateSparqlToSql(&state.GetContext(), sparql_text.GetString(), mapping_path_str.GetString());
		    return StringVector::AddString(result, sql);
	    });
}

void RegisterSparqlToSql(ExtensionLoader &loader) {
	ScalarFunction sparql_to_sql_sf("sparql_to_sql", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                SparqlToSql);
	sparql_to_sql_sf.SetFallible();
	CreateScalarFunctionInfo info(sparql_to_sql_sf);
	FunctionDescription desc;
	desc.description =
	    "Translate a SPARQL SELECT or ASK query into an equivalent SQL query, using an R2RML or YARRRML mapping "
	    "file(s) "
	    "in reverse. The mapping must be a full R2RML mapping (every TriplesMap has an rr:logicalTable or YARRRML "
	    "'sources' entry) - inside-out-only mappings are not accepted. Throws a detailed error naming the mapping "
	    "file(s), the SPARQL syntax problem, or the unsupported SPARQL construct on failure. Currently only the "
	    "'duckdb' SQL dialect is supported.";
	desc.examples.push_back(
	    "SELECT sparql_to_sql('SELECT ?e ?name WHERE { ?e <http://example.com/ns#name> ?name }', 'mapping.ttl')");
	info.descriptions.push_back(desc);
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
