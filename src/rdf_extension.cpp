#define DUCKDB_EXTENSION_MAIN

#include "rdf_extension.hpp"
#include "duckdb.hpp"
#include "include/I_triples_buffer.hpp"
#ifndef DUCK_RDF_NO_SPARQL
#include "include/sparql_reader.hpp"
#endif
#include "include/r2rml_copy.hpp"
#include "include/profile_rdf.hpp"
#include "include/pivot_rdf.hpp"
#include "include/read_rdf_prefixes.hpp"
#include "include/table_filter_eval.hpp"
#include "include/rdf_multi_file.hpp"
#include "include/rdf_triples_factory.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include "duckdb/common/file_system.hpp"
#include <atomic>
#include <mutex>
#include "include/string_util.hpp"

using namespace std;

#define STRICT_PARSING   "strict_parsing"
#define PREFIX_EXPANSION "prefix_expansion"
#define FILE_TYPE        "file_type"
#define FILENAME_PARAM   "filename"

namespace duckdb {

// Bind data: holds the expanded list of files (supports glob patterns)
struct RDFReaderBindData : public TableFunctionData {
	vector<string> file_paths;
	vector<idx_t> file_sizes;
	idx_t total_bytes = 0;
	// UNKNOWN means detect per-file from extension; set explicitly if file_type param given
	ITriplesBuffer::FileType file_type = ITriplesBuffer::UNKNOWN;
	bool strict_parsing = true;
	bool expand_prefixes = false;
	bool include_filenames = false;
};

// Global state: shared across all threads, tracks which file to process next
struct RDFReaderGlobalState : public GlobalTableFunctionState {
	std::mutex lock;
	idx_t next_file = 0;
	idx_t file_count = 0;
	std::atomic<uint64_t> bytes_consumed {0};
	idx_t total_bytes = 0;

	idx_t MaxThreads() const override {
		return file_count;
	}
};

// Local state: holds the active parser for this thread's current file
struct RDFReaderLocalState : public LocalTableFunctionState {
	std::unique_ptr<ITriplesBuffer> ib;
	vector<column_t> column_ids;
	optional_ptr<TableFilterSet> filters;
	string current_file;
};

// Rough average bytes-per-triple by format, used only as a planner hint.
static double AvgBytesPerTriple(ITriplesBuffer::FileType ft) {
	switch (ft) {
	case ITriplesBuffer::TURTLE:
	case ITriplesBuffer::TRIG:
		return 60.0;
	case ITriplesBuffer::XML:
		return 200.0;
	case ITriplesBuffer::NQUADS:
	case ITriplesBuffer::NTRIPLES:
	default:
		return 120.0;
	}
}

static idx_t EstimateTriplesForFile(idx_t size, ITriplesBuffer::FileType ft, bool compressed) {
	double bytes = (double)size;
	if (compressed) {
		bytes *= 5.0; // rough decompression multiplier
	}
	return (idx_t)(bytes / AvgBytesPerTriple(ft));
}

static unique_ptr<NodeStatistics> RDFReaderCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (const RDFReaderBindData &)*bind_data_p;
	idx_t estimated_triples = 0;
	for (idx_t i = 0; i < bind_data.file_paths.size(); i++) {
		const string &path = bind_data.file_paths[i];
		ITriplesBuffer::FileType ft = bind_data.file_type == ITriplesBuffer::UNKNOWN
		                                  ? ITriplesBuffer::DetectFileTypeFromPath(path)
		                                  : bind_data.file_type;
		bool compressed = ITriplesBuffer::IsCompressedPath(path);
		estimated_triples += EstimateTriplesForFile(bind_data.file_sizes[i], ft, compressed);
	}
	return make_uniq<NodeStatistics>(estimated_triples);
}

static unique_ptr<FunctionData> RDFReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RDFReaderBindData>();
	auto &fs = FileSystem::GetFileSystem(context);

	// Expand the input (a glob pattern or a LIST[VARCHAR] of paths/globs) to a
	// concrete list of files
	auto resolved_files = ResolveRDFFiles(context, input, "read_rdf");
	for (auto &info : resolved_files) {
		result->file_paths.push_back(std::move(info.path));
	}

	// Stat each matched file once, for cardinality/progress estimation.
	result->file_sizes.reserve(result->file_paths.size());
	for (auto &path : result->file_paths) {
		idx_t size = 0;
		try {
			auto h = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
			size = (idx_t)fs.GetFileSize(*h);
		} catch (std::exception &) {
			size = 0;
		}
		result->file_sizes.push_back(size);
		result->total_bytes += size;
	}

	// Optional explicit file type override — applied to all matched files
	auto file_type_param = input.named_parameters.find(FILE_TYPE);
	if (file_type_param != input.named_parameters.end()) {
		result->file_type = ITriplesBuffer::ParseFileTypeString(file_type_param->second.GetValue<string>());
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

	auto include_filenames_param = input.named_parameters.find(FILENAME_PARAM);
	if (include_filenames_param != input.named_parameters.end()) {
		result->include_filenames = include_filenames_param->second.GetValue<bool>();
	}

	names = {"graph", "subject", "predicate", "object", "object_datatype", "object_lang"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	if (result->include_filenames) {
		names.push_back("filename");
		return_types.push_back(LogicalType::VARCHAR);
	}
	return std::move(result);
}

// Creates the shared global state; called once before any threads start scanning
static unique_ptr<GlobalTableFunctionState> RDFReaderGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (RDFReaderBindData &)*input.bind_data;
	auto state = make_uniq<RDFReaderGlobalState>();
	state->file_count = bind_data.file_paths.size();
	state->total_bytes = bind_data.total_bytes;
	return state;
}

// Creates thread-local state; file opening is deferred to RDFReaderFunc
static unique_ptr<LocalTableFunctionState> RDFReaderInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                         GlobalTableFunctionState *global_state) {
	auto state = make_uniq<RDFReaderLocalState>();
	state->column_ids = input.column_ids;
	state->filters = input.filters;
	return state;
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
				if (bind_data.include_filenames) {
					// SetColumnIds() ignores col_ids >= 6, so we handle the filename column here.
					// Find which output slot corresponds to column index 6 (filename).
					for (idx_t i = 0; i < state.column_ids.size(); i++) {
						if (state.column_ids[i] == 6) {
							auto &vec = output.data[i];
							auto sv = StringVector::AddString(vec, state.current_file);
							for (idx_t row = 0; row < output.size(); row++) {
								FlatVector::GetData<string_t>(vec)[row] = sv;
							}
							break;
						}
					}
				}
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

		// The filename column (index 6) is synthetic - it isn't known to
		// ITriplesBuffer, so a filter on it has to be enforced here, once per
		// file. This also lets us skip parsing entire non-matching files.
		// TableFilterSet is keyed by position within column_ids (see
		// CreateTableFilterSet in plan_get.cpp), so find that position first.
		const TableFilter *filename_filter = nullptr;
		if (bind_data.include_filenames && state.filters) {
			for (idx_t i = 0; i < state.column_ids.size(); i++) {
				if (state.column_ids[i] == 6) {
					auto it = state.filters->filters.find(i);
					if (it != state.filters->filters.end()) {
						filename_filter = it->second.get();
					}
					break;
				}
			}
		}

		// Atomically claim the next file index
		idx_t file_idx;
		string file_path;
		while (true) {
			{
				std::lock_guard<std::mutex> lk(global_state.lock);
				if (global_state.next_file >= global_state.file_count) {
					return; // no more files; empty output signals done to DuckDB
				}
				file_idx = global_state.next_file++;
			}
			file_path = bind_data.file_paths[file_idx];
			if (!filename_filter || PassesFilter(filename_filter, file_path.data(), file_path.size(), false)) {
				break;
			}
		}

		// Open and start parsing the claimed file
		state.current_file = file_path;
		try {
			auto new_ib = OpenTriplesFile(file_path, bind_data.file_type, fs, bind_data.strict_parsing,
			                              bind_data.expand_prefixes);
			new_ib->SetProgressCounter(&global_state.bytes_consumed);
			new_ib->StartParse();
			new_ib->SetColumnIds(state.column_ids);
			new_ib->SetFilters(state.filters, state.column_ids);
			state.ib = std::move(new_ib);
		} catch (const std::runtime_error &re) {
			throw IOException(re.what());
		}
	}
}

static double RDFReaderProgress(ClientContext &context, const FunctionData *bind_data_p,
                                const GlobalTableFunctionState *global_state_p) {
	auto &global_state = (const RDFReaderGlobalState &)*global_state_p;
	if (global_state.total_bytes == 0) {
		return 100.0;
	}
	uint64_t consumed = global_state.bytes_consumed.load(std::memory_order_relaxed);
	double pct =
	    100.0 * (double)std::min<uint64_t>(consumed, global_state.total_bytes) / (double)global_state.total_bytes;
	return std::min(100.0, std::max(0.0, pct));
}

static void LoadInternal(ExtensionLoader &loader) {
	string extension_name = "read_rdf";
	TableFunction tf(extension_name, {LogicalType::VARCHAR}, RDFReaderFunc, RDFReaderBind, RDFReaderGlobalInit,
	                 RDFReaderInit);
	tf.named_parameters[STRICT_PARSING] = LogicalType::BOOLEAN;
	tf.named_parameters[PREFIX_EXPANSION] = LogicalType::BOOLEAN;
	tf.named_parameters[FILE_TYPE] = LogicalType::VARCHAR;
	tf.named_parameters[FILENAME_PARAM] = LogicalType::BOOLEAN;
	tf.projection_pushdown = true;
	tf.filter_pushdown = true;
	tf.cardinality = RDFReaderCardinality;
	tf.table_scan_progress = RDFReaderProgress;

	auto function_set = RegisterRDFFileListFunction(tf);
	CreateTableFunctionInfo info(function_set);
	FunctionDescription desc;
	desc.description =
	    "Read RDF triples from one or more files (Turtle, NTriples, NQuads, TriG, or RDF/XML) into a table with "
	    "columns graph, subject, predicate, object, object_datatype, and object_lang. Glob patterns and lists of "
	    "file paths are supported.";
	desc.examples.push_back("SELECT * FROM read_rdf('data.nt')");
	desc.examples.push_back("SELECT subject, predicate, object FROM read_rdf('*.ttl')");
	desc.examples.push_back("SELECT * FROM read_rdf(['a.nt', 'b.nt'])");
	desc.examples.push_back("SELECT * FROM read_rdf('data.rdf', file_type='rdf', strict_parsing=false)");
	info.descriptions.push_back(desc);
	loader.RegisterFunction(std::move(info));

	RegisterR2RMLCopy(loader);
#ifndef DUCK_RDF_NO_SPARQL
	RegisterSPARQLReader(loader);
#endif
	RegisterProfileRDF(loader);
	RegisterPivotRDF(loader);
	RegisterReadRDFPrefixes(loader);
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
