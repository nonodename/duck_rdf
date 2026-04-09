#include "include/read_rdf_prefixes.hpp"
#include "include/I_triples_buffer.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include <serd/serd.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#define PREFIXES_STRICT_PARSING    "strict_parsing"
#define PREFIXES_FILE_TYPE         "file_type"
#define PREFIXES_INCLUDE_FILENAMES "include_filenames"

using namespace std;

namespace duckdb {

// ============================================================
// Pre-computed output rows
// ============================================================

struct PrefixRow {
	std::string prefix;  // empty when prefix_is_null is true
	bool prefix_is_null; // true for @base declarations (no prefix name)
	std::string uri;
	bool is_base;
	std::string filename;
};

// Context passed as user_data to SERD callbacks
struct PrefixExtractContext {
	std::vector<PrefixRow> &rows;
	std::string filename;
	bool has_error;
	std::string error_message;
	bool strict_parsing;
};

// ============================================================
// SERD callbacks
// ============================================================

static SerdStatus PrefixBaseCallback(void *user_data, const SerdNode *uri) {
	auto *ctx = static_cast<PrefixExtractContext *>(user_data);
	PrefixRow row;
	row.prefix = "";
	row.prefix_is_null = true; // @base has no prefix name — emit SQL NULL
	row.uri = uri->buf ? std::string(reinterpret_cast<const char *>(uri->buf), uri->n_bytes) : "";
	row.is_base = true;
	row.filename = ctx->filename;
	ctx->rows.push_back(std::move(row));
	return SERD_SUCCESS;
}

static SerdStatus PrefixNameCallback(void *user_data, const SerdNode *name, const SerdNode *uri) {
	auto *ctx = static_cast<PrefixExtractContext *>(user_data);
	PrefixRow row;
	row.prefix = name->buf ? std::string(reinterpret_cast<const char *>(name->buf), name->n_bytes) : "";
	row.prefix_is_null = false;
	row.uri = uri->buf ? std::string(reinterpret_cast<const char *>(uri->buf), uri->n_bytes) : "";
	row.is_base = false;
	row.filename = ctx->filename;
	ctx->rows.push_back(std::move(row));
	return SERD_SUCCESS;
}

static SerdStatus NullStatementCallback(void * /*user_data*/, SerdStatementFlags /*flags*/, const SerdNode * /*graph*/,
                                        const SerdNode * /*subject*/, const SerdNode * /*predicate*/,
                                        const SerdNode * /*object*/, const SerdNode * /*object_datatype*/,
                                        const SerdNode * /*object_lang*/) {
	return SERD_SUCCESS;
}

static SerdStatus PrefixErrorCallback(void *user_data, const SerdError *error) {
	auto *ctx = static_cast<PrefixExtractContext *>(user_data);
	if (ctx->strict_parsing) {
		ctx->has_error = true;
		ctx->error_message = std::string("SERD parsing error in '") + ctx->filename + "', at line " +
		                     std::to_string(error->line) + ", column " + std::to_string(error->col);
		return SERD_FAILURE;
	}
	return SERD_SUCCESS;
}

// ============================================================
// Per-file extraction
// ============================================================

static void ExtractPrefixesFromFile(const string &file_path, FileSystem &fs, ITriplesBuffer::FileType ft,
                                    bool strict_parsing, std::vector<PrefixRow> &rows) {
	// File type is validated at bind time; only TURTLE and TRIG reach here.
	SerdSyntax syntax = (ft == ITriplesBuffer::TURTLE) ? SERD_TURTLE : SERD_TRIG;

	PrefixExtractContext ctx {rows, file_path, false, "", strict_parsing};

	std::unique_ptr<SerdEnv, decltype(&serd_env_free)> env(serd_env_new(nullptr), &serd_env_free);
	std::unique_ptr<SerdReader, decltype(&serd_reader_free)> reader(
	    serd_reader_new(syntax, &ctx, nullptr, &PrefixBaseCallback, &PrefixNameCallback, &NullStatementCallback,
	                    nullptr),
	    &serd_reader_free);

	serd_reader_set_strict(reader.get(), strict_parsing);
	serd_reader_set_error_sink(reader.get(), &PrefixErrorCallback, &ctx);

	std::unique_ptr<FileHandle> file_handle;
	try {
		file_handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);
	} catch (std::exception &ex) {
		throw IOException("Could not open file: " + file_path + ": " + ex.what());
	}

	// Bridge DuckDB FileHandle to SerdSource via non-capturing lambdas (C++11 compatible)
	auto duckdb_source = [](void *buf, size_t /*size*/, size_t nmemb, void *stream) -> size_t {
		auto fh = static_cast<FileHandle *>(stream);
		if (!fh)
			return 0;
		int64_t read = fh->Read(buf, (idx_t)nmemb);
		return (size_t)std::max<int64_t>(read, 0);
	};
	auto duckdb_error = [](void *) -> int {
		return 0;
	};

	const char *fp = file_path.c_str();
	serd_reader_start_source_stream(reader.get(), (SerdSource)duckdb_source, (SerdStreamErrorFunc)duckdb_error,
	                                file_handle.get(), (uint8_t *)fp, 4096U);

	SerdStatus st;
	do {
		st = serd_reader_read_chunk(reader.get());
	} while (st == SERD_SUCCESS);

	serd_reader_end_stream(reader.get());

	if (ctx.has_error && strict_parsing) {
		throw SyntaxException(ctx.error_message);
	}
}

// ============================================================
// Table function state
// ============================================================

struct RDFPrefixesBindData : public TableFunctionData {
	vector<string> file_paths;
	ITriplesBuffer::FileType file_type = ITriplesBuffer::UNKNOWN;
	bool strict_parsing = true;
	bool include_filenames = false;
};

struct RDFPrefixesGlobalState : public GlobalTableFunctionState {
	std::vector<PrefixRow> rows;
	std::atomic<idx_t> position {0};

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct RDFPrefixesLocalState : public LocalTableFunctionState {};

// ============================================================
// Bind
// ============================================================

static unique_ptr<FunctionData> RDFPrefixesBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RDFPrefixesBindData>();
	auto &fs = FileSystem::GetFileSystem(context);

	string pattern = input.inputs[0].GetValue<string>();
	auto glob_results = fs.Glob(pattern);
	if (glob_results.empty())
		throw IOException("No files found matching: " + pattern);
	for (auto &info : glob_results)
		result->file_paths.push_back(std::move(info.path));

	auto ft_it = input.named_parameters.find(PREFIXES_FILE_TYPE);
	if (ft_it != input.named_parameters.end())
		result->file_type = ITriplesBuffer::ParseFileTypeString(ft_it->second.GetValue<string>());

	auto sp_it = input.named_parameters.find(PREFIXES_STRICT_PARSING);
	if (sp_it != input.named_parameters.end())
		result->strict_parsing = sp_it->second.GetValue<bool>();

	// Validate file types at bind time so errors propagate cleanly.
	// Auto-detect per file when file_type=UNKNOWN.
	for (const auto &file_path : result->file_paths) {
		ITriplesBuffer::FileType ft = result->file_type;
		if (ft == ITriplesBuffer::UNKNOWN)
			ft = ITriplesBuffer::DetectFileTypeFromPath(file_path);
		if (ft == ITriplesBuffer::NTRIPLES || ft == ITriplesBuffer::NQUADS) {
			throw InvalidInputException(
			    "read_rdf_prefixes() does not support NTriples or NQuads format — these formats have no "
			    "prefix declarations (file: %s)",
			    file_path.c_str());
		}
		if (ft != ITriplesBuffer::TURTLE && ft != ITriplesBuffer::TRIG) {
			throw InvalidInputException(
			    "read_rdf_prefixes() only supports Turtle (.ttl) and TriG (.trig) formats (file: %s)",
			    file_path.c_str());
		}
	}

	auto fn_it = input.named_parameters.find(PREFIXES_INCLUDE_FILENAMES);
	if (fn_it != input.named_parameters.end())
		result->include_filenames = fn_it->second.GetValue<bool>();

	names = {"prefix", "uri", "is_base"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN};
	if (result->include_filenames) {
		names.push_back("filename");
		return_types.push_back(LogicalType::VARCHAR);
	}
	return std::move(result);
}

// ============================================================
// Global init — all parsing happens here
// ============================================================

static unique_ptr<GlobalTableFunctionState> RDFPrefixesGlobalInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &bind_data = (RDFPrefixesBindData &)*input.bind_data;
	auto &fs = FileSystem::GetFileSystem(context);

	auto state = make_uniq<RDFPrefixesGlobalState>();

	for (auto &file_path : bind_data.file_paths) {
		ITriplesBuffer::FileType ft = bind_data.file_type;
		if (ft == ITriplesBuffer::UNKNOWN)
			ft = ITriplesBuffer::DetectFileTypeFromPath(file_path);

		try {
			ExtractPrefixesFromFile(file_path, fs, ft, bind_data.strict_parsing, state->rows);
		} catch (const std::runtime_error &re) {
			throw IOException(re.what());
		}
	}

	return state;
}

// ============================================================
// Local init
// ============================================================

static unique_ptr<LocalTableFunctionState> RDFPrefixesLocalInit(ExecutionContext & /*context*/,
                                                                TableFunctionInitInput & /*input*/,
                                                                GlobalTableFunctionState * /*global*/) {
	return make_uniq<RDFPrefixesLocalState>();
}

// ============================================================
// Scan — emit pre-computed rows
// ============================================================

static void RDFPrefixesFunc(ClientContext & /*context*/, TableFunctionInput &input, DataChunk &output) {
	auto &global = (RDFPrefixesGlobalState &)*input.global_state;
	auto &bind_data = (RDFPrefixesBindData &)*input.bind_data;

	idx_t out_idx = 0;
	const idx_t capacity = STANDARD_VECTOR_SIZE;

	while (out_idx < capacity) {
		idx_t row_idx = global.position.fetch_add(1, std::memory_order_relaxed);
		if (row_idx >= global.rows.size())
			break;

		const PrefixRow &row = global.rows[row_idx];

		// @base declarations have no prefix name — emit SQL NULL
		if (row.prefix_is_null) {
			output.SetValue(0, out_idx, Value());
		} else {
			output.SetValue(0, out_idx, Value(row.prefix));
		}
		output.SetValue(1, out_idx, Value(row.uri));
		output.SetValue(2, out_idx, Value::BOOLEAN(row.is_base));
		if (bind_data.include_filenames)
			output.SetValue(3, out_idx, Value(row.filename));

		out_idx++;
	}

	output.SetCardinality(out_idx);
}

// ============================================================
// Registration
// ============================================================

void RegisterReadRDFPrefixes(ExtensionLoader &loader) {
	TableFunction tf("read_rdf_prefixes", {LogicalType::VARCHAR}, RDFPrefixesFunc, RDFPrefixesBind,
	                 RDFPrefixesGlobalInit, RDFPrefixesLocalInit);
	tf.named_parameters[PREFIXES_STRICT_PARSING] = LogicalType::BOOLEAN;
	tf.named_parameters[PREFIXES_FILE_TYPE] = LogicalType::VARCHAR;
	tf.named_parameters[PREFIXES_INCLUDE_FILENAMES] = LogicalType::BOOLEAN;
	loader.RegisterFunction(tf);
}

} // namespace duckdb
