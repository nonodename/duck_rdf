#include "include/execute_sparql.hpp"
#include "include/sparql_to_sql.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>

namespace duckdb {

// bind_replace splices the SQL translated from sparql_text/mapping_path into
// this query's own plan as a subquery - the same mechanism DuckDB's built-in
// query() table function uses (duckdb/src/function/table/query_function.cpp)
// - rather than executing it as an opaque nested query. The binder recurses
// into Bind() on the returned TableRef (bind_table_function.cpp), so filters,
// projections, and joins written around the execute_sparql(...) call are
// planned and optimized together with the translated SQL, not bolted on
// afterwards.
static unique_ptr<TableRef> ExecuteSparqlBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto sparql_text = input.inputs[0].GetValue<string>();
	auto mapping_path = input.inputs[1].GetValue<string>();

	std::string sql = TranslateSparqlToSql(context, sparql_text, mapping_path);

	Parser parser(context.GetParserOptions());
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InternalException("execute_sparql: translated SQL did not parse back into a single SELECT statement "
		                        "(generated SQL: %s)",
		                        sql.c_str());
	}
	auto select_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
	return make_uniq<SubqueryRef>(std::move(select_stmt));
}

void RegisterExecuteSparql(ExtensionLoader &loader) {
	TableFunction tf("execute_sparql", {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr, nullptr);
	tf.bind_replace = ExecuteSparqlBindReplace;

	CreateTableFunctionInfo info(tf);
	FunctionDescription desc;
	desc.description =
	    "Translate a SPARQL SELECT or ASK query into SQL via an R2RML or YARRRML mapping (like sparql_to_sql), then "
	    "run it directly as a table. Unlike calling sparql_to_sql() and pasting the result into a second query, "
	    "execute_sparql() splices the translated SQL into this query's own plan before optimization, so filters, "
	    "projections, and joins written around the call are planned together with it. Same mapping requirements "
	    "and error messages as sparql_to_sql().";
	desc.examples.push_back("SELECT * FROM execute_sparql('SELECT ?e ?name WHERE { ?e "
	                        "<http://example.com/ns#name> ?name }', 'mapping.ttl')");
	info.descriptions.push_back(desc);
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
