#include "include/pivot_rdf.hpp"
#include "include/rdf_profiler.hpp"
#include "include/I_triples_buffer.hpp"
#include "include/serd_buffer.hpp"
#include "include/xml_buffer.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#define PIVOT_FILE_TYPE      "file_type"
#define PIVOT_STRICT_PARSING "strict_parsing"
#define PIVOT_PREFIX_EXPAND  "prefix_expansion"

using namespace std;

namespace duckdb {

// ============================================================
// Column kind — used at scan time to know how to build Values
// ============================================================

enum class PivotColKind {
	SCALAR,   // single typed value (VARCHAR, HUGEINT, BOOLEAN, …)
	LANG_MAP, // MAP(VARCHAR, VARCHAR) for all-lang-tagged predicates
	LIST,     // LIST(<element_type>) for multi-valued predicates
};

// Describes one predicate column in the output schema.
struct PivotColumn {
	std::string predicate;
	LogicalType col_type;  // the DuckDB type of the column itself
	LogicalType elem_type; // for LIST columns: the element type; else same as col_type
	PivotColKind kind;
	// For UNION elem_type: the member list, in sorted order.
	child_list_t<LogicalType> union_members;
};

// ============================================================
// Helper: derive DuckDB LogicalType for a predicate column
// ============================================================

// Normalise a profile type-name to a DuckDB LogicalType.
// IRI and BLANK both become VARCHAR (they are stored as URI strings).
static LogicalType TypeNameToLogical(const std::string &name) {
	if (name == "IRI" || name == "BLANK" || name == "VARCHAR")
		return LogicalType::VARCHAR;
	if (name == "BOOLEAN")
		return LogicalType::BOOLEAN;
	if (name == "TINYINT")
		return LogicalType::TINYINT;
	if (name == "UTINYINT")
		return LogicalType::UTINYINT;
	if (name == "SMALLINT")
		return LogicalType::SMALLINT;
	if (name == "USMALLINT")
		return LogicalType::USMALLINT;
	if (name == "INTEGER")
		return LogicalType::INTEGER;
	if (name == "UINTEGER")
		return LogicalType::UINTEGER;
	if (name == "BIGINT")
		return LogicalType::BIGINT;
	if (name == "UBIGINT")
		return LogicalType::UBIGINT;
	if (name == "HUGEINT")
		return LogicalType::HUGEINT;
	if (name == "FLOAT")
		return LogicalType::FLOAT;
	if (name == "DOUBLE")
		return LogicalType::DOUBLE;
	if (name == "DECIMAL")
		return LogicalType::DOUBLE; // safe fallback
	if (name == "DATE")
		return LogicalType::DATE;
	if (name == "TIME")
		return LogicalType::TIME;
	if (name == "TIMESTAMP")
		return LogicalType::TIMESTAMP;
	if (name == "TIMESTAMP WITH TIME ZONE")
		return LogicalType::TIMESTAMP_TZ;
	if (name == "INTERVAL")
		return LogicalType::INTERVAL;
	if (name == "BLOB")
		return LogicalType::BLOB;
	// Unknown XSD URI or anything else — store as VARCHAR
	return LogicalType::VARCHAR;
}

// Build a PivotColumn from a (predicate, PredicateProfile) pair.
static PivotColumn BuildPivotColumn(const std::string &predicate, const PredicateProfile &profile) {
	PivotColumn col;
	col.predicate = predicate;

	// Use MAP(VARCHAR,VARCHAR) when every literal is language-tagged and there are
	// no plain/typed literals.  The map key is the language tag, value is the string.
	bool use_lang_map = profile.has_lang_tagged && !profile.has_non_lang_literal;
	if (use_lang_map) {
		col.kind = PivotColKind::LANG_MAP;
		col.elem_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
		col.col_type = col.elem_type;
		return col;
	}

	// Collect distinct normalised LogicalTypes (IRI/BLANK → VARCHAR).
	// Use sorted type-name strings for determinism.
	std::vector<std::string> type_names;
	type_names.reserve(profile.type_stats.size());
	for (const auto &kv : profile.type_stats)
		type_names.push_back(kv.first);
	std::sort(type_names.begin(), type_names.end());

	// Deduplicate normalised types (IRI and BLANK both become VARCHAR).
	std::vector<LogicalType> distinct_types;
	std::vector<std::string> distinct_names;
	for (const auto &name : type_names) {
		LogicalType lt = TypeNameToLogical(name);
		bool already = false;
		for (const auto &dt : distinct_types) {
			if (dt == lt) {
				already = true;
				break;
			}
		}
		if (!already) {
			distinct_types.push_back(lt);
			// Use a normalised label for UNION member names
			if (lt == LogicalType::VARCHAR)
				distinct_names.push_back("varchar");
			else
				distinct_names.push_back(name);
		}
	}

	// Derive element type.
	LogicalType elem;
	child_list_t<LogicalType> union_members;
	if (distinct_types.size() == 1) {
		elem = distinct_types[0];
	} else {
		// Multiple distinct types → UNION
		for (size_t i = 0; i < distinct_types.size(); i++)
			union_members.push_back(make_pair(distinct_names[i], distinct_types[i]));
		elem = LogicalType::UNION(union_members);
	}

	// Wrap in LIST if multi-valued.
	if (profile.is_multi_valued) {
		col.kind = PivotColKind::LIST;
		col.elem_type = elem;
		col.col_type = LogicalType::LIST(elem);
		col.union_members = union_members;
	} else {
		col.kind = PivotColKind::SCALAR;
		col.elem_type = elem;
		col.col_type = elem;
		col.union_members = union_members;
	}
	return col;
}

// ============================================================
// Per-column accumulator used during the streaming scan pass
// ============================================================

struct PivotColAccum {
	// For LANG_MAP: key=lang, value=string.
	std::unordered_map<std::string, std::string> lang_map;
	// For SCALAR and LIST: collected Values.
	std::vector<Value> values;
	// Track the UNION tag index for each stored value (for LIST<UNION> columns).
	std::vector<idx_t> union_tags;

	void Reset() {
		lang_map.clear();
		values.clear();
		union_tags.clear();
	}
};

// ============================================================
// Convert a raw string triple field to a typed Value
// ============================================================

static Value StringToValue(const std::string &str, const std::string &datatype, const std::string &lang,
                           const LogicalType &target) {
	if (target == LogicalType::VARCHAR)
		return Value(str);
	if (target == LogicalType::BOOLEAN)
		return Value::BOOLEAN(str == "true" || str == "1");
	if (target == LogicalType::TINYINT) {
		try {
			return Value::TINYINT(static_cast<int8_t>(std::stoi(str)));
		} catch (...) {
			return Value(str);
		}
	}
	if (target == LogicalType::UTINYINT) {
		try {
			return Value::UTINYINT(static_cast<uint8_t>(std::stoul(str)));
		} catch (...) {
			return Value(str);
		}
	}
	if (target == LogicalType::SMALLINT) {
		try {
			return Value::SMALLINT(static_cast<int16_t>(std::stoi(str)));
		} catch (...) {
			return Value(str);
		}
	}
	if (target == LogicalType::USMALLINT) {
		try {
			return Value::USMALLINT(static_cast<uint16_t>(std::stoul(str)));
		} catch (...) {
			return Value(str);
		}
	}
	if (target == LogicalType::INTEGER) {
		try {
			return Value::INTEGER(static_cast<int32_t>(std::stoi(str)));
		} catch (...) {
			return Value(str);
		}
	}
	if (target == LogicalType::UINTEGER) {
		try {
			return Value::UINTEGER(static_cast<uint32_t>(std::stoul(str)));
		} catch (...) {
			return Value(str);
		}
	}
	if (target == LogicalType::BIGINT) {
		try {
			return Value::BIGINT(static_cast<int64_t>(std::stoll(str)));
		} catch (...) {
			return Value(str);
		}
	}
	if (target == LogicalType::UBIGINT) {
		try {
			return Value::UBIGINT(static_cast<uint64_t>(std::stoull(str)));
		} catch (...) {
			return Value(str);
		}
	}
	if (target == LogicalType::HUGEINT) {
		try {
			return Value::HUGEINT(static_cast<int64_t>(std::stoll(str)));
		} catch (...) {
			return Value(str);
		}
	}
	if (target == LogicalType::FLOAT) {
		try {
			return Value::FLOAT(std::stof(str));
		} catch (...) {
			return Value(str);
		}
	}
	if (target == LogicalType::DOUBLE) {
		try {
			return Value::DOUBLE(std::stod(str));
		} catch (...) {
			return Value(str);
		}
	}
	// For DATE, TIME, TIMESTAMP, BLOB, INTERVAL, etc. fall back to VARCHAR.
	// A full implementation would cast via DuckDB's cast machinery.
	return Value(str);
}

// ============================================================
// Build a final column Value from an accumulator entry
// ============================================================

static Value BuildColValue(const PivotColAccum &accum, const PivotColumn &col) {
	switch (col.kind) {
	case PivotColKind::LANG_MAP: {
		// Build MAP(VARCHAR, VARCHAR)
		duckdb::vector<Value> keys, vals;
		keys.reserve(accum.lang_map.size());
		vals.reserve(accum.lang_map.size());
		// Sort by key for deterministic output
		std::vector<std::pair<std::string, std::string>> sorted(accum.lang_map.begin(), accum.lang_map.end());
		std::sort(sorted.begin(), sorted.end());
		for (auto &kv : sorted) {
			keys.emplace_back(Value(kv.first));
			vals.emplace_back(Value(kv.second));
		}
		return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, keys, vals);
	}
	case PivotColKind::SCALAR: {
		if (accum.values.empty())
			return Value(col.col_type); // NULL
		return accum.values[0];
	}
	case PivotColKind::LIST: {
		if (accum.values.empty()) {
			duckdb::vector<Value> empty;
			return Value::LIST(col.elem_type, empty);
		}
		if (col.union_members.empty()) {
			// LIST of simple type
			duckdb::vector<Value> list_vals(accum.values.begin(), accum.values.end());
			return Value::LIST(col.elem_type, list_vals);
		} else {
			// LIST of UNION — wrap each value with its UNION tag
			duckdb::vector<Value> union_vals;
			union_vals.reserve(accum.values.size());
			for (size_t i = 0; i < accum.values.size(); i++) {
				idx_t tag = (i < accum.union_tags.size()) ? accum.union_tags[i] : 0;
				union_vals.emplace_back(Value::UNION(col.union_members, static_cast<uint8_t>(tag), accum.values[i]));
			}
			return Value::LIST(col.elem_type, union_vals);
		}
	}
	}
	return Value(col.col_type); // unreachable, keeps compiler happy
}

// ============================================================
// Open a file for streaming (mirrors OpenFile in rdf_extension.cpp)
// ============================================================

static unique_ptr<ITriplesBuffer> PivotOpenFile(const string &file_path, ITriplesBuffer::FileType ft, FileSystem &fs,
                                                bool strict_parsing, bool expand_prefixes) {
	if (ft == ITriplesBuffer::UNKNOWN)
		ft = ITriplesBuffer::DetectFileTypeFromPath(file_path);
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

// ============================================================
// Table function state
// ============================================================

struct PivotRDFBindData : public TableFunctionData {
	vector<string> file_paths;
	ITriplesBuffer::FileType file_type = ITriplesBuffer::UNKNOWN;
	bool strict_parsing = true;
	bool expand_prefixes = false;

	// Ordered list of predicate columns (alphabetical by predicate URI)
	std::vector<PivotColumn> columns;
	// Fast lookup: predicate URI → index in columns[]
	std::unordered_map<std::string, idx_t> pred_to_col;
};

struct PivotRDFGlobalState : public GlobalTableFunctionState {
	std::mutex lock;
	idx_t next_file = 0;
	idx_t file_count = 0;

	idx_t MaxThreads() const override {
		return file_count;
	}
};

struct PivotRDFLocalState : public LocalTableFunctionState {
	// Active file parser producing raw 6-col triples
	std::unique_ptr<ITriplesBuffer> ib;

	// Intermediate buffer for raw triples (6 VARCHAR columns)
	DataChunk raw_chunk;
	idx_t raw_chunk_pos = 0;

	// Current (graph, subject) being accumulated
	std::string current_graph;
	std::string current_subject;
	bool has_pending = false;

	// Per-predicate accumulators (indexed same as PivotRDFBindData::columns)
	std::vector<PivotColAccum> col_accum;

	bool raw_chunk_initialized = false;
};

// ============================================================
// Bind — Pass 1: profile files, derive schema
// ============================================================

static unique_ptr<FunctionData> PivotRDFBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<PivotRDFBindData>();
	auto &fs = FileSystem::GetFileSystem(context);

	string pattern = input.inputs[0].GetValue<string>();
	auto glob_results = fs.Glob(pattern);
	if (glob_results.empty())
		throw IOException("No files found matching: " + pattern);
	for (auto &info : glob_results)
		result->file_paths.push_back(std::move(info.path));

	auto ft_it = input.named_parameters.find(PIVOT_FILE_TYPE);
	if (ft_it != input.named_parameters.end())
		result->file_type = ITriplesBuffer::ParseFileTypeString(ft_it->second.GetValue<string>());

	auto sp_it = input.named_parameters.find(PIVOT_STRICT_PARSING);
	if (sp_it != input.named_parameters.end())
		result->strict_parsing = sp_it->second.GetValue<bool>();

	auto pe_it = input.named_parameters.find(PIVOT_PREFIX_EXPAND);
	if (pe_it != input.named_parameters.end())
		result->expand_prefixes = pe_it->second.GetValue<bool>();

	// --- Pass 1: profile all files to discover predicates and types ---
	RDFProfileAccumulator accumulator;
	for (auto &file_path : result->file_paths) {
		ITriplesBuffer::FileType ft = result->file_type;
		if (ft == ITriplesBuffer::UNKNOWN)
			ft = ITriplesBuffer::DetectFileTypeFromPath(file_path);
		try {
			switch (ft) {
			case ITriplesBuffer::TURTLE:
			case ITriplesBuffer::NTRIPLES:
			case ITriplesBuffer::NQUADS:
			case ITriplesBuffer::TRIG:
				ProfileFileSerd(file_path, fs, ft, result->strict_parsing, accumulator);
				break;
			case ITriplesBuffer::XML:
				ProfileFileXML(file_path, fs, result->strict_parsing, accumulator);
				break;
			default:
				throw IOException("Cannot determine file type for: " + file_path);
			}
		} catch (const std::runtime_error &re) {
			throw IOException(re.what());
		}
	}

	// Build sorted list of predicate columns
	const auto &profiles = accumulator.GetProfiles();
	std::vector<std::string> pred_uris;
	pred_uris.reserve(profiles.size());
	for (const auto &kv : profiles)
		pred_uris.push_back(kv.first);
	std::sort(pred_uris.begin(), pred_uris.end());

	result->columns.reserve(pred_uris.size());
	for (const auto &pred : pred_uris) {
		result->columns.push_back(BuildPivotColumn(pred, profiles.at(pred)));
	}
	for (idx_t i = 0; i < result->columns.size(); i++)
		result->pred_to_col[result->columns[i].predicate] = i;

	// Build return schema: graph, subject, then one col per predicate
	names.push_back("graph");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("subject");
	return_types.push_back(LogicalType::VARCHAR);
	for (const auto &col : result->columns) {
		names.push_back(col.predicate);
		return_types.push_back(col.col_type);
	}

	return std::move(result);
}

// ============================================================
// Global init
// ============================================================

static unique_ptr<GlobalTableFunctionState> PivotRDFGlobalInit(ClientContext & /*context*/,
                                                               TableFunctionInitInput &input) {
	auto &bind_data = (PivotRDFBindData &)*input.bind_data;
	auto state = make_uniq<PivotRDFGlobalState>();
	state->file_count = bind_data.file_paths.size();
	return state;
}

// ============================================================
// Local init
// ============================================================

static unique_ptr<LocalTableFunctionState> PivotRDFLocalInit(ExecutionContext & /*context*/,
                                                             TableFunctionInitInput &input,
                                                             GlobalTableFunctionState * /*global*/) {
	auto &bind_data = (PivotRDFBindData &)*input.bind_data;
	auto state = make_uniq<PivotRDFLocalState>();
	state->col_accum.resize(bind_data.columns.size());
	return state;
}

// ============================================================
// Helper: emit the current accumulated (graph, subject) row
// ============================================================

static void EmitPendingRow(PivotRDFLocalState &state, const PivotRDFBindData &bind_data, DataChunk &output,
                           idx_t out_idx) {
	// col 0: graph
	output.SetValue(0, out_idx, Value(state.current_graph));
	// col 1: subject
	output.SetValue(1, out_idx, Value(state.current_subject));
	// cols 2..N: predicate values
	for (idx_t i = 0; i < bind_data.columns.size(); i++) {
		output.SetValue(static_cast<idx_t>(2) + i, out_idx, BuildColValue(state.col_accum[i], bind_data.columns[i]));
	}
}

// ============================================================
// Helper: accumulate one raw triple into the per-column buffers
// ============================================================

static void AccumulateTriple(PivotRDFLocalState &state, const PivotRDFBindData &bind_data, const std::string &predicate,
                             const std::string &object, const std::string &datatype, const std::string &lang) {
	auto it = bind_data.pred_to_col.find(predicate);
	if (it == bind_data.pred_to_col.end())
		return; // unknown predicate (shouldn't happen after a consistent profile)
	idx_t col_idx = it->second;
	const PivotColumn &col = bind_data.columns[col_idx];
	PivotColAccum &accum = state.col_accum[col_idx];

	switch (col.kind) {
	case PivotColKind::LANG_MAP: {
		// key = lang tag (may be empty string if somehow mixed in)
		accum.lang_map[lang] = object;
		break;
	}
	case PivotColKind::SCALAR: {
		// Only keep the first value (schema says single-valued)
		if (accum.values.empty()) {
			if (col.union_members.empty()) {
				accum.values.push_back(StringToValue(object, datatype, lang, col.elem_type));
			} else {
				// UNION scalar: find tag index
				ObjectKind ok =
				    lang.empty() ? (datatype.empty() ? ObjectKind::LITERAL : ObjectKind::LITERAL) : ObjectKind::LITERAL;
				// Determine the type name from context
				std::string type_name = XsdToDuckDBType(datatype, lang, ok);
				LogicalType lt = TypeNameToLogical(type_name);
				idx_t tag = 0;
				for (idx_t m = 0; m < col.union_members.size(); m++) {
					if (col.union_members[m].second == lt) {
						tag = m;
						break;
					}
				}
				Value inner = StringToValue(object, datatype, lang, lt);
				accum.values.push_back(Value::UNION(col.union_members, static_cast<uint8_t>(tag), inner));
				accum.union_tags.push_back(tag);
			}
		}
		break;
	}
	case PivotColKind::LIST: {
		if (col.union_members.empty()) {
			accum.values.push_back(StringToValue(object, datatype, lang, col.elem_type));
		} else {
			// LIST<UNION>: determine tag, store raw value and tag separately
			ObjectKind ok = ObjectKind::LITERAL;
			std::string type_name = XsdToDuckDBType(datatype, lang, ok);
			LogicalType lt = TypeNameToLogical(type_name);
			idx_t tag = 0;
			for (idx_t m = 0; m < col.union_members.size(); m++) {
				if (col.union_members[m].second == lt) {
					tag = m;
					break;
				}
			}
			accum.values.push_back(StringToValue(object, datatype, lang, lt));
			accum.union_tags.push_back(tag);
		}
		break;
	}
	}
}

// ============================================================
// Helper: reset all column accumulators
// ============================================================

static void ResetAccumulators(PivotRDFLocalState &state) {
	for (auto &a : state.col_accum)
		a.Reset();
}

// ============================================================
// Scan function — Pass 2: stream triples, pivot by subject
// ============================================================

static void PivotRDFFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = (PivotRDFLocalState &)*input.local_state;
	auto &global_state = (PivotRDFGlobalState &)*input.global_state;
	auto &bind_data = (PivotRDFBindData &)*input.bind_data;
	auto &fs = FileSystem::GetFileSystem(context);

	// Lazily initialise the raw 6-col chunk used as an intermediate triple buffer.
	if (!state.raw_chunk_initialized) {
		vector<LogicalType> raw_types(6, LogicalType::VARCHAR);
		state.raw_chunk.Initialize(Allocator::Get(context), raw_types);
		// All 6 columns needed for pivot (predicate, object, datatype, lang, subject, graph)
		vector<column_t> all_cols = {0, 1, 2, 3, 4, 5};
		state.raw_chunk_initialized = true;
	}

	idx_t out_idx = 0;

	while (out_idx < STANDARD_VECTOR_SIZE) {
		// ── Refill raw_chunk if exhausted ──────────────────────────────────────
		if (state.raw_chunk_pos >= state.raw_chunk.size()) {
			if (state.ib) {
				state.raw_chunk.Reset();
				state.ib->PopulateChunk(state.raw_chunk);
				state.raw_chunk_pos = 0;

				if (state.raw_chunk.size() == 0) {
					// File exhausted — flush the last pending subject
					if (state.has_pending) {
						EmitPendingRow(state, bind_data, output, out_idx++);
						ResetAccumulators(state);
						state.has_pending = false;
					}
					state.ib.reset();
					// Fall through to claim next file
				} else {
					// Continue processing the newly filled chunk
					continue;
				}
			}

			// Claim the next file
			idx_t file_idx;
			{
				std::lock_guard<std::mutex> lk(global_state.lock);
				if (global_state.next_file >= global_state.file_count) {
					break; // no more files
				}
				file_idx = global_state.next_file++;
			}

			const string &file_path = bind_data.file_paths[file_idx];
			try {
				auto new_ib = PivotOpenFile(file_path, bind_data.file_type, fs, bind_data.strict_parsing,
				                            bind_data.expand_prefixes);
				new_ib->StartParse();
				// Request all 6 columns
				vector<column_t> all_cols = {0, 1, 2, 3, 4, 5};
				new_ib->SetColumnIds(all_cols);
				state.ib = std::move(new_ib);
			} catch (const std::runtime_error &re) {
				throw IOException(re.what());
			}

			// Refill raw_chunk from the new file
			state.raw_chunk.Reset();
			state.ib->PopulateChunk(state.raw_chunk);
			state.raw_chunk_pos = 0;

			if (state.raw_chunk.size() == 0) {
				// Empty file
				state.ib.reset();
				continue;
			}
		}

		// ── Process one triple from raw_chunk ──────────────────────────────────
		idx_t row = state.raw_chunk_pos++;

		// raw_chunk columns: 0=graph, 1=subject, 2=predicate, 3=object, 4=datatype, 5=lang
		auto ReadStr = [&](idx_t col) -> std::string {
			auto &vec = state.raw_chunk.data[col];
			auto *data = FlatVector::GetData<string_t>(vec);
			auto &validity = FlatVector::Validity(vec);
			if (!validity.RowIsValid(row))
				return {};
			return data[row].GetString();
		};

		std::string graph = ReadStr(0);
		std::string subject = ReadStr(1);
		std::string predicate = ReadStr(2);
		std::string object = ReadStr(3);
		std::string datatype = ReadStr(4);
		std::string lang = ReadStr(5);

		// ── Subject boundary detection ─────────────────────────────────────────
		if (state.has_pending && (graph != state.current_graph || subject != state.current_subject)) {
			EmitPendingRow(state, bind_data, output, out_idx++);
			ResetAccumulators(state);
			state.current_graph = graph;
			state.current_subject = subject;
			// has_pending stays true (we immediately start a new subject)
		} else if (!state.has_pending) {
			state.current_graph = graph;
			state.current_subject = subject;
			state.has_pending = true;
		}

		AccumulateTriple(state, bind_data, predicate, object, datatype, lang);
	}

	output.SetCardinality(out_idx);
}

// ============================================================
// Registration
// ============================================================

void RegisterPivotRDF(ExtensionLoader &loader) {
	TableFunction tf("pivot_rdf", {LogicalType::VARCHAR}, PivotRDFFunc, PivotRDFBind, PivotRDFGlobalInit,
	                 PivotRDFLocalInit);
	tf.named_parameters[PIVOT_FILE_TYPE] = LogicalType::VARCHAR;
	tf.named_parameters[PIVOT_STRICT_PARSING] = LogicalType::BOOLEAN;
	tf.named_parameters[PIVOT_PREFIX_EXPAND] = LogicalType::BOOLEAN;
	loader.RegisterFunction(tf);
}

} // namespace duckdb
