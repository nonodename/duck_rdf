#include "include/sparql_to_sql.hpp"
#include "include/r2rml_copy.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include <r2rml/R2RMLMapping.h>
#include <sparql-parser/ParseError.h>
#include <sparql-parser/Parser.h>
#include <sparql2sql/DuckDbDialect.h>
#include <sparql2sql/Translator.h>
#include <sparql2sql/TranslationError.h>
#include <memory>

namespace duckdb {

static std::string DescribeParseError(const sparql::ParseError &e) {
	return "SPARQL parse error: " + e.message() + " (line " + std::to_string(e.line()) + ", column " +
	       std::to_string(e.column()) + ", near '" + e.nearText() + "')";
}

inline void SparqlToSql(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &sparql_vector = args.data[0];
	auto &mapping_vector = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    sparql_vector, mapping_vector, result, args.size(), [&](string_t sparql_text, string_t mapping_path_str) {
		    std::string mapping_path = mapping_path_str.GetString();

		    auto &fs = FileSystem::GetFileSystem(state.GetContext());
		    if (!fs.FileExists(mapping_path)) {
			    throw IOException("Mapping file not found: " + mapping_path);
		    }

		    r2rml::R2RMLMapping mapping;
		    try {
			    mapping = ParseR2RMLOrYarrrmlMapping(mapping_path);
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
			    query = parser.parseString(sparql_text.GetString());
		    } catch (const sparql::ParseError &e) {
			    throw InvalidInputException(DescribeParseError(e));
		    } catch (const std::exception &e) {
			    throw InvalidInputException("SPARQL parse error: %s", e.what());
		    }

		    std::string sql;
		    try {
			    sparql2sql::DuckDbDialect dialect;
			    sql = sparql2sql::translateQuery(*query, mapping, dialect);
		    } catch (const std::exception &e) {
			    throw InvalidInputException("SPARQL-to-SQL translation error: %s", e.what());
		    }

		    return StringVector::AddString(result, sql);
	    });
}

void RegisterSparqlToSql(ExtensionLoader &loader) {
	ScalarFunction sparql_to_sql_sf("sparql_to_sql", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                SparqlToSql);
	CreateScalarFunctionInfo info(sparql_to_sql_sf);
	FunctionDescription desc;
	desc.description =
	    "Translate a SPARQL SELECT or ASK query into an equivalent SQL query, using an R2RML or YARRRML mapping file "
	    "in reverse. The mapping must be a full R2RML mapping (every TriplesMap has an rr:logicalTable or YARRRML "
	    "'sources' entry) - inside-out-only mappings are not accepted. Throws a detailed error naming the mapping "
	    "file, the SPARQL syntax problem, or the unsupported SPARQL construct on failure. Currently only the "
	    "'duckdb' SQL dialect is supported.";
	desc.examples.push_back(
	    "SELECT sparql_to_sql('SELECT ?e ?name WHERE { ?e <http://example.com/ns#name> ?name }', 'mapping.ttl')");
	info.descriptions.push_back(desc);
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
