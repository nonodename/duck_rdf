#include "include/sparql_parser.hpp"
#include "include/sparql_to_sql.hpp"
#include "include/r2rml_copy.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <r2rml/R2RMLMapping.h>
#include <sparql-parser/ParseError.h>
#include <sparql-parser/Parser.h>

#include <mutex>
#include <string>

namespace duckdb {

// Shared, database-wide (not per-connection) toggle state. Neither
// parse_function nor parser_override receive a ClientContext, so this can't
// live in per-connection state - it's reachable from the ParserExtension
// callbacks via ParserExtensionInfo, and from enable/disable_sparql_parser's
// bind callbacks via TableFunction::function_info (both point at the same
// object, set up once in RegisterSparqlParser).
struct SparqlParserState : public ParserExtensionInfo, public TableFunctionInfo {
	std::mutex mutex;
	bool enabled = false;
	std::string mapping_path;
};

// parser_override receives the raw statement text exactly as submitted,
// including a trailing ';' if the caller typed one (DuckDB's own parser
// handles statement-terminating semicolons internally rather than
// pre-stripping them). SPARQL has no such trailing terminator - a lone ';' at
// the very end is not part of the query - so it must be dropped before
// probing/translating, or every SPARQL statement typed the normal SQL way
// (ending in ';') would fail to parse as SPARQL and silently fall through.
static std::string StripTrailingSemicolon(const std::string &query) {
	auto end = query.find_last_not_of(" \t\r\n");
	if (end == std::string::npos) {
		return query;
	}
	if (query[end] == ';') {
		end = end == 0 ? std::string::npos : query.find_last_not_of(" \t\r\n", end - 1);
	}
	return end == std::string::npos ? std::string() : query.substr(0, end + 1);
}

static ParserOverrideResult SparqlParserOverride(ParserExtensionInfo *info, const string &query,
                                                 ParserOptions &options) {
	auto &state = *static_cast<SparqlParserState *>(info);

	bool enabled;
	std::string mapping_path;
	{
		std::lock_guard<std::mutex> lock(state.mutex);
		enabled = state.enabled;
		mapping_path = state.mapping_path;
	}
	if (!enabled) {
		return ParserOverrideResult();
	}

	std::string sparql_text = StripTrailingSemicolon(query);

	// Probe whether this text is SPARQL-shaped at all: if it doesn't even
	// parse as SPARQL, this almost certainly wasn't meant for us - fall
	// through silently so ordinary SQL keeps working while the toggle is on.
	try {
		sparql::Parser probe;
		probe.parseString(sparql_text);
	} catch (const sparql::ParseError &) {
		return ParserOverrideResult();
	} catch (const std::exception &) {
		return ParserOverrideResult();
	}

	// It is SPARQL - from here on, a failure is a real, SPARQL-specific
	// error (bad/missing mapping, unsupported construct), not a fallback
	// signal, so surface it directly instead of a confusing SQL syntax error.
	try {
		std::string sql = TranslateSparqlToSql(nullptr, sparql_text, mapping_path);
		Parser sql_parser(options);
		sql_parser.ParseQuery(sql);
		return ParserOverrideResult(std::move(sql_parser.statements));
	} catch (std::exception &e) {
		return ParserOverrideResult(e);
	}
}

// enable_sparql_parser()/disable_sparql_parser() both just report the
// resulting enabled state as a single-row result, so CALL has something to
// show - the real effect is the SparqlParserState mutation done in bind.
struct SparqlParserToggleData : public TableFunctionData {
	bool enabled_after;
};

struct SparqlParserToggleState : public GlobalTableFunctionState {
	bool emitted = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<GlobalTableFunctionState> SparqlParserToggleInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<SparqlParserToggleState>();
}

static void SparqlParserToggleFunc(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &gstate = (SparqlParserToggleState &)*input.global_state;
	if (gstate.emitted) {
		output.SetCardinality(0);
		return;
	}
	gstate.emitted = true;
	auto &bind_data = (SparqlParserToggleData &)*input.bind_data;
	output.SetValue(0, 0, Value::BOOLEAN(bind_data.enabled_after));
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> EnableSparqlParserBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto mapping_path = input.inputs[0].GetValue<string>();

	auto &fs = FileSystem::GetFileSystem(context);
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
		    "Mapping '%s' is not a valid full R2RML mapping. enable_sparql_parser() requires every TriplesMap to "
		    "declare an rr:logicalTable (or YARRRML 'sources') naming the table/view to query, the same as "
		    "sparql_to_sql() and execute_sparql().",
		    mapping_path.c_str());
	}

	auto &state = *static_cast<SparqlParserState *>(input.info.get());
	{
		std::lock_guard<std::mutex> lock(state.mutex);
		state.enabled = true;
		state.mapping_path = mapping_path;
	}
	// parser_override only runs when allow_parser_override_extension is not
	// DEFAULT - flip it to STRICT so bare SPARQL statements actually reach it.
	DBConfig::GetConfig(context).SetOptionByName("allow_parser_override_extension", Value("STRICT"));

	names = {"sparql_parser_enabled"};
	return_types = {LogicalType::BOOLEAN};
	auto result = make_uniq<SparqlParserToggleData>();
	result->enabled_after = true;
	return std::move(result);
}

static unique_ptr<FunctionData> DisableSparqlParserBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto &state = *static_cast<SparqlParserState *>(input.info.get());
	{
		std::lock_guard<std::mutex> lock(state.mutex);
		state.enabled = false;
		state.mapping_path.clear();
	}
	DBConfig::GetConfig(context).SetOptionByName("allow_parser_override_extension", Value("DEFAULT"));

	names = {"sparql_parser_enabled"};
	return_types = {LogicalType::BOOLEAN};
	auto result = make_uniq<SparqlParserToggleData>();
	result->enabled_after = false;
	return std::move(result);
}

void RegisterSparqlParser(ExtensionLoader &loader) {
	auto state = make_shared_ptr<SparqlParserState>();

	ParserExtension parser_extension;
	parser_extension.parser_override = SparqlParserOverride;
	parser_extension.parser_info = shared_ptr_cast<SparqlParserState, ParserExtensionInfo>(state);
	ParserExtension::Register(DBConfig::GetConfig(loader.GetDatabaseInstance()), parser_extension);

	TableFunction enable_tf("enable_sparql_parser", {LogicalType::VARCHAR}, SparqlParserToggleFunc,
	                        EnableSparqlParserBind, SparqlParserToggleInit);
	enable_tf.function_info = shared_ptr_cast<SparqlParserState, TableFunctionInfo>(state);
	CreateTableFunctionInfo enable_info(enable_tf);
	FunctionDescription enable_desc;
	enable_desc.description =
	    "Enable treating raw SPARQL SELECT/ASK statements (not wrapped in sparql_to_sql()/execute_sparql()) as "
	    "first-class statements, translated on the fly via the given R2RML or YARRRML mapping - the same mapping "
	    "requirements as sparql_to_sql()/execute_sparql(). This is a database-wide toggle (shared by all "
	    "connections to this database, not just the current session); ordinary SQL statements keep working "
	    "unaffected while it is enabled. Use disable_sparql_parser() to turn it back off.";
	enable_desc.examples.push_back("CALL enable_sparql_parser('mapping.ttl')");
	enable_info.descriptions.push_back(enable_desc);
	loader.RegisterFunction(std::move(enable_info));

	TableFunction disable_tf("disable_sparql_parser", {}, SparqlParserToggleFunc, DisableSparqlParserBind,
	                         SparqlParserToggleInit);
	disable_tf.function_info = shared_ptr_cast<SparqlParserState, TableFunctionInfo>(state);
	CreateTableFunctionInfo disable_info(disable_tf);
	FunctionDescription disable_desc;
	disable_desc.description = "Disable raw SPARQL statement support previously turned on by enable_sparql_parser().";
	disable_desc.examples.push_back("CALL disable_sparql_parser()");
	disable_info.descriptions.push_back(disable_desc);
	loader.RegisterFunction(std::move(disable_info));
}

} // namespace duckdb
