#include "include/r2rml_copy.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/file_system.hpp"
#include <serd/serd.h>
#include <r2rml/R2RMLMapping.h>
#include <r2rml/R2RMLParser.h>
#include <r2rml/MapSQLRow.h>
#include <r2rml/SQLConnection.h>
#include <r2rml/SQLResultSet.h>
#include <r2rml/SQLRow.h>
#include <r2rml/SQLValue.h>
#include <r2rml/StringSQLValue.h>
#include <r2rml/TriplesMap.h>
#include <map>
#include <unordered_map>

using namespace std;

#define MAPPING_OPTION          "mapping"
#define RDF_FORMAT_OPTION       "rdf_format"
#define IGNORE_NON_FATAL_ERRORS "ignore_non_fatal_errors"
#define IGNORE_CASE_OPTION      "ignore_case"

namespace duckdb {

inline void CanCallInsideOut(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(name_vector, result, args.size(), [&](string_t name) {
		auto &fs = FileSystem::GetFileSystem(state.GetContext());
		if (!fs.FileExists(name.GetString())) {
			return false;
		}
		r2rml::R2RMLParser parser;
		r2rml::R2RMLMapping mapping = parser.parse(name.GetString());
		return mapping.isValidInsideOut();
	});
}

inline void IsValidR2RML(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(name_vector, result, args.size(), [&](string_t name) {
		auto &fs = FileSystem::GetFileSystem(state.GetContext());
		if (!fs.FileExists(name.GetString())) {
			return false;
		}
		r2rml::R2RMLParser parser;
		r2rml::R2RMLMapping mapping = parser.parse(name.GetString());
		return mapping.isValid();
	});
}

static SerdSyntax ParseRdfFormat(const std::string &fmt) {
	std::string x = fmt;
	for (auto &c : x)
		c = (char)tolower(c);
	if (x == "ttl" || x == "turtle")
		return SERD_TURTLE;
	if (x == "nq" || x == "nquads")
		return SERD_NQUADS;
	if (x == "nt" || x == "ntriples")
		return SERD_NTRIPLES;
	throw InvalidInputException("Unknown rdf_format '%s'. Valid values: ntriples, turtle, nquads.", fmt.c_str());
}

// Lazily wraps a DuckDB Value; type and string representation are computed on
// first access so columns unreferenced by any TriplesMap are never converted.
class DataChunkSQLValue : public r2rml::SQLValue {
public:
	explicit DataChunkSQLValue(Value val) : val_(std::move(val)) {
	}

	bool isNull() const override {
		return val_.IsNull();
	}
	Type type() const override {
		ensureConverted();
		return type_;
	}
	const std::string &asString() const override {
		ensureConverted();
		return string_;
	}
	std::unique_ptr<SQLValue> clone() const override {
		return std::unique_ptr<SQLValue>(new DataChunkSQLValue(val_));
	}

private:
	Value val_;
	mutable Type type_ {Type::Null};
	mutable std::string string_;
	mutable bool converted_ {false};

	void ensureConverted() const {
		if (converted_) {
			return;
		}
		converted_ = true;
		if (val_.IsNull()) {
			return;
		}
		switch (val_.type().id()) {
		case LogicalTypeId::BOOLEAN:
			type_ = Type::Boolean;
			string_ = val_.GetValue<bool>() ? "true" : "false";
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
			type_ = Type::Integer;
			string_ = std::to_string(val_.GetValue<int32_t>());
			break;
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::HUGEINT:
			type_ = Type::String;
			string_ = val_.ToString();
			break;
		case LogicalTypeId::FLOAT:
			type_ = Type::Double;
			string_ = std::to_string(static_cast<double>(val_.GetValue<float>()));
			break;
		case LogicalTypeId::DOUBLE:
			type_ = Type::Double;
			string_ = std::to_string(val_.GetValue<double>());
			break;
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::BLOB:
			type_ = Type::String;
			string_ = val_.GetValue<std::string>();
			break;
		default:
			type_ = Type::String;
			string_ = val_.ToString();
			break;
		}
	}
};

// Zero-copy proxy over a single row of a DuckDB DataChunk.  Values are wrapped
// in DataChunkSQLValue on demand; columns not referenced by any TriplesMap are
// never materialised.  The DataChunk and col_index must outlive this object.
class DataChunkSQLRow : public r2rml::SQLRow {
public:
	DataChunkSQLRow(const DataChunk &chunk, idx_t row, const std::unordered_map<std::string, idx_t> &col_index,
	                bool ignore_case)
	    : chunk_(chunk), row_(row), col_index_(col_index), ignore_case_(ignore_case) {
	}

	std::unique_ptr<r2rml::SQLValue> getValue(const std::string &name) const override {
		auto it = col_index_.find(normalise(name));
		if (it == col_index_.end()) {
			return std::unique_ptr<r2rml::SQLValue>(new r2rml::StringSQLValue());
		}
		return std::unique_ptr<r2rml::SQLValue>(new DataChunkSQLValue(chunk_.GetValue(it->second, row_)));
	}

	bool isNull(const std::string &name) const override {
		auto it = col_index_.find(normalise(name));
		if (it == col_index_.end()) {
			return true;
		}
		return chunk_.GetValue(it->second, row_).IsNull();
	}

	// Materialise into a MapSQLRow when a stable copy is needed.
	std::unique_ptr<r2rml::SQLRow> clone() const override {
		std::map<std::string, std::unique_ptr<r2rml::SQLValue>> cols;
		for (const auto &kv : col_index_) {
			cols[kv.first] = std::unique_ptr<r2rml::SQLValue>(new DataChunkSQLValue(chunk_.GetValue(kv.second, row_)));
		}
		return std::unique_ptr<r2rml::SQLRow>(new r2rml::MapSQLRow(std::move(cols)));
	}

private:
	const DataChunk &chunk_;
	idx_t row_;
	const std::unordered_map<std::string, idx_t> &col_index_;
	bool ignore_case_;

	std::string normalise(const std::string &name) const {
		if (!ignore_case_) {
			return name;
		}
		std::string lower = name;
		for (auto &c : lower) {
			c = (char)tolower(c);
		}
		return lower;
	}
};

// Materialised result set: holds all rows fetched from a DuckDB query.
class VectorSQLResultSet : public r2rml::SQLResultSet {
public:
	explicit VectorSQLResultSet(std::vector<r2rml::MapSQLRow> rows) : rows_(std::move(rows)) {
	}
	bool next() override {
		return ++cursor_ < static_cast<int>(rows_.size());
	}
	const r2rml::SQLRow &getCurrentRow() const override {
		return rows_[static_cast<size_t>(cursor_)];
	}

private:
	std::vector<r2rml::MapSQLRow> rows_;
	int cursor_ = -1;
};

// SQLConnection backed by the live DuckDB instance via a fresh Connection.
// Used for full R2RML mode where processDatabase() runs the mapping's SQL queries.
class ClientContextSQLConnection : public r2rml::SQLConnection {
public:
	ClientContextSQLConnection(ClientContext &ctx, bool ignore_case) : context_(ctx), ignore_case_(ignore_case) {
	}

	std::unique_ptr<r2rml::SQLResultSet> execute(const std::string &sql) override {
		Connection conn(*context_.db);
		// When ignore_case is set, lowercase the SQL so that table names from
		// rr:tableName (e.g. "EMP") match DuckDB's lowercase-folded identifiers.
		std::string exec_sql = sql;
		if (ignore_case_) {
			for (auto &ch : exec_sql) {
				ch = (char)tolower(ch);
			}
		}
		auto result = conn.Query(exec_sql);
		if (result->HasError()) {
			throw InternalException("R2RML query error: " + result->GetError());
		}
		std::vector<r2rml::MapSQLRow> rows;
		while (true) {
			auto chunk = result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			for (idx_t r = 0; r < chunk->size(); r++) {
				std::map<std::string, std::unique_ptr<r2rml::SQLValue>> cols;
				for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
					std::string name = result->ColumnName(c);
					if (ignore_case_) {
						for (auto &ch : name) {
							ch = (char)tolower(ch);
						}
					} else {
						for (auto &ch : name) {
							ch = (char)toupper(ch);
						}
					}
					cols[name] = std::unique_ptr<r2rml::SQLValue>(new DataChunkSQLValue(chunk->GetValue(c, r)));
				}
				rows.emplace_back(std::move(cols));
			}
		}
		return unique_ptr<r2rml::SQLResultSet>(new VectorSQLResultSet(std::move(rows)));
	}

	std::string getDefaultSchema() override {
		return "main";
	}

private:
	ClientContext &context_;
	bool ignore_case_;
};

// Stub connection for inside-out mode.  isValidInsideOut() guarantees that no
// referencing object maps are present, so execute() should never be called.
struct NullSQLConnection : public r2rml::SQLConnection {
	std::unique_ptr<r2rml::SQLResultSet> execute(const std::string &) override {
		throw InternalException("SQL queries are not supported in inside-out R2RML mode");
	}
};

// Serd write sink: streams output sequentially to a DuckDB FileHandle.
static size_t serdFileHandleSink(const void *buf, size_t len, void *stream) {
	static_cast<FileHandle *>(stream)->Write(const_cast<void *>(buf), static_cast<idx_t>(len));
	return len;
}

struct R2RMLWriteBindData : public FunctionData {
	std::string mapping_file_path;
	std::shared_ptr<r2rml::R2RMLMapping> mapping;
	bool inside_out_mode = false;
	std::vector<std::string> column_names; // case-normalised; consumed by copy_to_sink
	std::vector<LogicalType> sql_types;
	SerdSyntax output_syntax = SERD_NTRIPLES;
	bool ignore_non_fatal_errors = true;
	bool ignore_case = false;

	unique_ptr<FunctionData> Copy() const override {
		auto c = make_uniq<R2RMLWriteBindData>();
		c->mapping_file_path = mapping_file_path;
		c->mapping = mapping;
		c->inside_out_mode = inside_out_mode;
		c->column_names = column_names;
		c->sql_types = sql_types;
		c->output_syntax = output_syntax;
		c->ignore_non_fatal_errors = ignore_non_fatal_errors;
		c->ignore_case = ignore_case;
		return c;
	}
	bool Equals(const FunctionData &other) const override {
		return mapping_file_path == other.Cast<R2RMLWriteBindData>().mapping_file_path;
	}
};

struct R2RMLWriteGlobalState : public GlobalFunctionData {
	unique_ptr<FileHandle> file_handle;
	SerdEnv *serd_env = nullptr;
	SerdWriter *serd_writer = nullptr;

	~R2RMLWriteGlobalState() {
		if (serd_writer) {
			serd_writer_free(serd_writer);
			serd_writer = nullptr;
		}
		if (serd_env) {
			serd_env_free(serd_env);
			serd_env = nullptr;
		}
	}
};

struct R2RMLWriteLocalState : public LocalFunctionData {};

static void R2RMLCopyOptions(ClientContext &, CopyOptionsInput &input) {
	input.options[MAPPING_OPTION] = CopyOption(LogicalType::VARCHAR);
	input.options[RDF_FORMAT_OPTION] = CopyOption(LogicalType::VARCHAR);
	input.options[IGNORE_NON_FATAL_ERRORS] = CopyOption(LogicalType::BOOLEAN);
	input.options[IGNORE_CASE_OPTION] = CopyOption(LogicalType::BOOLEAN);
}

static unique_ptr<FunctionData> R2RMLCopyToBind(ClientContext &context, CopyFunctionBindInput &input,
                                                const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto &options = input.info.options;

	// mapping option is required
	auto mapping_it = options.find(MAPPING_OPTION);
	if (mapping_it == options.end() || mapping_it->second.empty()) {
		throw InvalidInputException("r2rml format requires a 'mapping' option specifying the R2RML mapping file.");
	}
	std::string mapping_path = mapping_it->second[0].GetValue<std::string>();

	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.FileExists(mapping_path)) {
		throw IOException("R2RML mapping file not found: " + mapping_path);
	}

	bool ignore_nfe = true;
	auto nfe_it = options.find(IGNORE_NON_FATAL_ERRORS);
	if (nfe_it != options.end() && !nfe_it->second.empty()) {
		ignore_nfe = nfe_it->second[0].GetValue<bool>();
	}

	bool ignore_case = false;
	auto ic_it = options.find(IGNORE_CASE_OPTION);
	if (ic_it != options.end() && !ic_it->second.empty()) {
		ignore_case = ic_it->second[0].GetValue<bool>();
	}

	r2rml::R2RMLParser parser;
	std::shared_ptr<r2rml::R2RMLMapping> mapping;
	try {
		mapping = std::make_shared<r2rml::R2RMLMapping>(parser.parse(mapping_path, ignore_nfe));
	} catch (const std::runtime_error &e) {
		throw InvalidInputException("R2RML mapping parse error: %s", e.what());
	}

	bool inside_out = mapping->isValidInsideOut();
	if (!inside_out && !mapping->isValid()) {
		throw InvalidInputException("R2RML mapping '%s' is not valid.", mapping_path.c_str());
	}

	SerdSyntax syntax = SERD_NTRIPLES;
	auto fmt_it = options.find(RDF_FORMAT_OPTION);
	if (fmt_it != options.end() && !fmt_it->second.empty()) {
		syntax = ParseRdfFormat(fmt_it->second[0].GetValue<std::string>());
	}

	auto result = make_uniq<R2RMLWriteBindData>();
	result->mapping_file_path = mapping_path;
	result->mapping = mapping;
	result->inside_out_mode = inside_out;
	result->sql_types = sql_types;
	result->output_syntax = syntax;
	result->ignore_non_fatal_errors = ignore_nfe;
	result->ignore_case = ignore_case;

	for (const auto &name : names) {
		std::string normalised = name;
		if (ignore_case) {
			for (auto &c : normalised) {
				c = (char)tolower(c);
			}
		} else {
			for (auto &c : normalised) {
				c = (char)toupper(c);
			}
		}
		result->column_names.push_back(std::move(normalised));
	}

	return std::move(result);
}

static unique_ptr<GlobalFunctionData> R2RMLCopyToInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                                  const string &file_path) {
	auto &bind = bind_data.Cast<R2RMLWriteBindData>();
	auto state = make_uniq<R2RMLWriteGlobalState>();

	auto &fs = FileSystem::GetFileSystem(context);
	state->file_handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);

	state->serd_env = serd_env_new(nullptr);
	if (!state->serd_env) {
		throw InternalException("Failed to create Serd environment for RDF output.");
	}

	state->serd_writer = serd_writer_new(bind.output_syntax, (SerdStyle)0, state->serd_env, nullptr, serdFileHandleSink,
	                                     state->file_handle.get());
	if (!state->serd_writer) {
		throw InternalException("Failed to create Serd writer for RDF output.");
	}

	return std::move(state);
}

static unique_ptr<LocalFunctionData> R2RMLCopyToInitializeLocal(ExecutionContext &, FunctionData &) {
	return make_uniq<R2RMLWriteLocalState>();
}

static void R2RMLCopyToSink(ExecutionContext &, FunctionData &bind_data, GlobalFunctionData &gstate,
                            LocalFunctionData &, DataChunk &input) {
	auto &bind = bind_data.Cast<R2RMLWriteBindData>();
	if (!bind.inside_out_mode) {
		return; // full R2RML mode: rows from COPY SELECT are ignored
	}

	auto &global = gstate.Cast<R2RMLWriteGlobalState>();
	NullSQLConnection null_conn;

	// Build column-name → index map once per chunk; reused for every row.
	std::unordered_map<std::string, idx_t> col_index;
	col_index.reserve(input.ColumnCount());
	for (idx_t col = 0; col < input.ColumnCount(); col++) {
		col_index[bind.column_names[col]] = col;
	}

	for (idx_t row = 0; row < input.size(); row++) {
		DataChunkSQLRow sql_row(input, row, col_index, bind.ignore_case);
		for (const auto &tm : bind.mapping->triplesMaps) {
			if (tm) {
				tm->generateTriples(sql_row, *global.serd_writer, *bind.mapping, null_conn);
			}
		}
	}
}

static void R2RMLCopyToCombine(ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
}

static void R2RMLCopyToFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &bind = bind_data.Cast<R2RMLWriteBindData>();
	auto &global = gstate.Cast<R2RMLWriteGlobalState>();

	if (!bind.inside_out_mode) {
		// full R2RML mode: run the mapping's SQL queries against the live database
		try {
			ClientContextSQLConnection conn(context, bind.ignore_case);
			bind.mapping->processDatabase(conn, *global.serd_writer);
		} catch (const std::runtime_error &e) {
			throw IOException(std::string("R2RML processing error: ") + e.what());
		}
	}

	serd_writer_finish(global.serd_writer);
}

static CopyFunctionExecutionMode R2RMLCopyExecutionMode(bool, bool) {
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

void RegisterR2RMLCopy(ExtensionLoader &loader) {
	auto can_call_inside_out_scalar_function =
	    ScalarFunction("can_call_inside_out", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, CanCallInsideOut);
	loader.RegisterFunction(can_call_inside_out_scalar_function);

	auto is_valid_r2rml_scalar_function =
	    ScalarFunction("is_valid_r2rml", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, IsValidR2RML);
	loader.RegisterFunction(is_valid_r2rml_scalar_function);

	CopyFunction copy_func("r2rml");
	copy_func.extension = "nt";
	copy_func.copy_options = R2RMLCopyOptions;
	copy_func.copy_to_bind = R2RMLCopyToBind;
	copy_func.copy_to_initialize_global = R2RMLCopyToInitializeGlobal;
	copy_func.copy_to_initialize_local = R2RMLCopyToInitializeLocal;
	copy_func.copy_to_sink = R2RMLCopyToSink;
	copy_func.copy_to_combine = R2RMLCopyToCombine;
	copy_func.copy_to_finalize = R2RMLCopyToFinalize;
	copy_func.execution_mode = R2RMLCopyExecutionMode;
	loader.RegisterFunction(copy_func);
}

} // namespace duckdb
