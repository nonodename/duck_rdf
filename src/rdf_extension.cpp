#define DUCKDB_EXTENSION_MAIN

#include "rdf_extension.hpp"
#include "duckdb.hpp"
#include "include/serd_buffer.hpp"
#include "include/xml_buffer.hpp"
#include "include/I_triples_buffer.hpp"
#include "include/sparql_reader.hpp"
#include "include/r2rml_copy.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include "duckdb/common/file_system.hpp"
#include <mutex>

using namespace std;

#define STRICT_PARSING   "strict_parsing"
#define PREFIX_EXPANSION "prefix_expansion"
#define FILE_TYPE        "file_type"

namespace duckdb {

static ITriplesBuffer::FileType ConvertLabelToFileType(const std::string &s) {
	std::string x = s;
	for (auto &c : x)
		c = (char)tolower(c);
	if (x == "ttl" || x == "turtle")
		return ITriplesBuffer::TURTLE;
	if (x == "nq" || x == "nquads")
		return ITriplesBuffer::NQUADS;
	if (x == "nt" || x == "ntriples")
		return ITriplesBuffer::NTRIPLES;
	if (x == "trig")
		return ITriplesBuffer::TRIG;
	if (x == "rdf" || x == "xml")
		return ITriplesBuffer::XML;
	return ITriplesBuffer::UNKNOWN;
}

static ITriplesBuffer::FileType DetectFileTypeFromPath(const std::string &path) {
	auto pos = path.rfind('.');
	if (pos == std::string::npos)
		return ITriplesBuffer::UNKNOWN;
	std::string ext = path.substr(pos + 1);
	return ConvertLabelToFileType(ext);
}

static ITriplesBuffer::FileType ParseFileTypeString(const std::string &s) {
	ITriplesBuffer::FileType ft = ConvertLabelToFileType(s);
	if (ft == ITriplesBuffer::UNKNOWN)
		throw InvalidInputException("Unknown file_type override: '%s'", s.c_str());
	return ft;
}

// Bind data: holds the expanded list of files (supports glob patterns)
struct RDFReaderBindData : public TableFunctionData {
	vector<string> file_paths;
	// UNKNOWN means detect per-file from extension; set explicitly if file_type param given
	ITriplesBuffer::FileType file_type = ITriplesBuffer::UNKNOWN;
	bool strict_parsing = true;
	bool expand_prefixes = false;
};

// Global state: shared across all threads, tracks which file to process next
struct RDFReaderGlobalState : public GlobalTableFunctionState {
	std::mutex lock;
	idx_t next_file = 0;
	idx_t file_count = 0;

	idx_t MaxThreads() const override {
		return file_count;
	}
};

// Local state: holds the active parser for this thread's current file
struct RDFReaderLocalState : public LocalTableFunctionState {
	std::unique_ptr<ITriplesBuffer> ib;
	vector<column_t> column_ids;
	string current_file;
};

static unique_ptr<FunctionData> RDFReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RDFReaderBindData>();
	auto &fs = FileSystem::GetFileSystem(context);

	// Expand the input (which may be a glob pattern) to a concrete list of files
	string pattern = input.inputs[0].GetValue<string>();
	auto glob_results = fs.Glob(pattern);
	if (glob_results.empty()) {
		throw IOException("No files found matching: " + pattern);
	}
	for (auto &info : glob_results) {
		result->file_paths.push_back(std::move(info.path));
	}

	// Optional explicit file type override — applied to all matched files
	auto file_type_param = input.named_parameters.find(FILE_TYPE);
	if (file_type_param != input.named_parameters.end()) {
		result->file_type = ParseFileTypeString(file_type_param->second.GetValue<string>());
	} else {
		result->file_type = ITriplesBuffer::UNKNOWN; // detect per-file from extension
	}

	auto strict_parsing_param = input.named_parameters.find(STRICT_PARSING);
	if (strict_parsing_param != input.named_parameters.end()) {
		result->strict_parsing = strict_parsing_param->second.GetValue<bool>();
	} else {
		result->strict_parsing = true;
	}

	auto prefix_expansion_param = input.named_parameters.find(PREFIX_EXPANSION);
	if (prefix_expansion_param != input.named_parameters.end()) {
		result->expand_prefixes = prefix_expansion_param->second.GetValue<bool>();
	} else {
		result->expand_prefixes = false;
	}

	names = {"graph", "subject", "predicate", "object", "object_datatype", "object_lang"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(result);
}

// Creates the shared global state; called once before any threads start scanning
static unique_ptr<GlobalTableFunctionState> RDFReaderGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (RDFReaderBindData &)*input.bind_data;
	auto state = make_uniq<RDFReaderGlobalState>();
	state->file_count = bind_data.file_paths.size();
	return state;
}

// Creates thread-local state; file opening is deferred to RDFReaderFunc
static unique_ptr<LocalTableFunctionState> RDFReaderInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                         GlobalTableFunctionState *global_state) {
	auto state = make_uniq<RDFReaderLocalState>();
	state->column_ids = input.column_ids;
	return state;
}

// Opens a single file and returns the appropriate parser buffer
static unique_ptr<ITriplesBuffer> OpenFile(const string &file_path, ITriplesBuffer::FileType ft, FileSystem &fs,
                                           bool strict_parsing, bool expand_prefixes) {
	if (ft == ITriplesBuffer::UNKNOWN) {
		ft = DetectFileTypeFromPath(file_path);
	}
	switch (ft) {
	case ITriplesBuffer::TURTLE:
	case ITriplesBuffer::NQUADS:
	case ITriplesBuffer::NTRIPLES:
	case ITriplesBuffer::TRIG:
		return make_uniq<SerdBuffer>(file_path, "", &fs, strict_parsing, expand_prefixes, ft);
	case ITriplesBuffer::XML:
		return make_uniq<XMLBuffer>(file_path, "", &fs, strict_parsing, expand_prefixes, ft);
	default:
		throw IOException("Cannot determine file type for: " + file_path);
	}
}

static void RDFReaderFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = (RDFReaderLocalState &)*input.local_state;
	auto &global_state = (RDFReaderGlobalState &)*input.global_state;
	auto &bind_data = (RDFReaderBindData &)*input.bind_data;
	auto &fs = FileSystem::GetFileSystem(context);

	while (true) {
		// If we have an active buffer, try to get more rows from it
		if (state.ib) {
			state.ib->PopulateChunk(output);
			if (output.size() > 0) {
				return;
			}
			if (!bind_data.strict_parsing && state.ib->GetSkipCount() > 0) {
				// If we're skipping bad rows, log how many we skipped for this file
				auto &logger = Logger::Get(context);
				auto count = state.ib->GetSkipCount();
				if (count > 1) {
					logger.WriteLog("rdf_extension", LogLevel::LOG_WARNING, "Skipped %d malformed rows in file: %s",
					                count, state.current_file.c_str());
				} else {
					logger.WriteLog("rdf_extension", LogLevel::LOG_WARNING, "Skipped 1 malformed row in file: %s",
					                state.current_file.c_str());
				}
			}
			// Buffer exhausted — drop it and claim the next file
			state.ib.reset();
		}

		// Atomically claim the next file index
		idx_t file_idx;
		{
			std::lock_guard<std::mutex> lk(global_state.lock);
			if (global_state.next_file >= global_state.file_count) {
				return; // no more files; empty output signals done to DuckDB
			}
			file_idx = global_state.next_file++;
		}

		// Open and start parsing the claimed file
		const string &file_path = bind_data.file_paths[file_idx];
		state.current_file = file_path;
		try {
			auto new_ib =
			    OpenFile(file_path, bind_data.file_type, fs, bind_data.strict_parsing, bind_data.expand_prefixes);
			new_ib->StartParse();
			new_ib->SetColumnIds(state.column_ids);
			state.ib = std::move(new_ib);
		} catch (const std::runtime_error &re) {
			throw IOException(re.what());
		}
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	string extension_name = "read_rdf";
	TableFunction tf(extension_name, {LogicalType::VARCHAR}, RDFReaderFunc, RDFReaderBind, RDFReaderGlobalInit,
	                 RDFReaderInit);
	tf.named_parameters[STRICT_PARSING] = LogicalType::BOOLEAN;
	tf.named_parameters[PREFIX_EXPANSION] = LogicalType::BOOLEAN;
	tf.named_parameters[FILE_TYPE] = LogicalType::VARCHAR;
	tf.projection_pushdown = true;
	loader.RegisterFunction(tf);

	RegisterR2RMLCopy(loader);
	RegisterSPARQLReader(loader);
}

void RdfExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RdfExtension::Name() {
	return "rdf";
}

std::string RdfExtension::Version() const {
#ifdef EXT_VERSION_RDF
	return EXT_VERSION_RDF;
#else
	return "0.0.1-unknown";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(rdf, loader) {
	duckdb::LoadInternal(loader);
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
