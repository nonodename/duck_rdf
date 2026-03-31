#include "include/profile_rdf.hpp"
#include "include/rdf_profiler.hpp"
#include "include/I_triples_buffer.hpp"
#include "include/string_util.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <stdexcept>
#include <vector>

#define PROFILE_FILE_TYPE      "file_type"
#define PROFILE_STRICT_PARSING "strict_parsing"

using namespace std;

namespace duckdb {

// Build a MAP(VARCHAR, UBIGINT) value from parallel key/value arrays.
static Value BuildVarcharBigintMap(const std::vector<std::string> &keys, const std::vector<uint64_t> &values) {
	D_ASSERT(keys.size() == values.size());
	duckdb::vector<Value> key_vals, val_vals;
	key_vals.reserve(keys.size());
	val_vals.reserve(values.size());
	for (size_t i = 0; i < keys.size(); i++) {
		key_vals.emplace_back(Value(keys[i]));
		val_vals.emplace_back(Value::UBIGINT(values[i]));
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::UBIGINT, key_vals, val_vals);
}

// Build a MAP(VARCHAR, VARCHAR) value from parallel key/value arrays.
static Value BuildVarcharVarcharMap(const std::vector<std::string> &keys, const std::vector<std::string> &values) {
	D_ASSERT(keys.size() == values.size());
	duckdb::vector<Value> key_vals, val_vals;
	key_vals.reserve(keys.size());
	val_vals.reserve(values.size());
	for (size_t i = 0; i < keys.size(); i++) {
		key_vals.emplace_back(Value(keys[i]));
		val_vals.emplace_back(Value(values[i]));
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, key_vals, val_vals);
}

// ============================================================
// Pre-computed output rows
// ============================================================

struct ProfileRow {
	std::string predicate;
	std::vector<std::string> sorted_types; // sorted unique type names
	// per-type stats, in the same order as sorted_types
	std::vector<uint64_t> counts;
	std::vector<std::string> mins;
	std::vector<std::string> maxs;
	uint64_t graph_count;
	uint64_t subject_count;
};

static std::vector<ProfileRow> BuildRows(const RDFProfileAccumulator &acc) {
	std::vector<ProfileRow> rows;
	rows.reserve(acc.GetProfiles().size());

	for (const auto &pred_kv : acc.GetProfiles()) {
		const std::string &predicate = pred_kv.first;
		const PredicateProfile &profile = pred_kv.second;
		ProfileRow row;
		row.predicate = predicate;
		row.graph_count = profile.graphs.size();
		row.subject_count = profile.subjects.size();

		// Collect and sort type names for deterministic output
		std::vector<std::string> types;
		types.reserve(profile.type_stats.size());
		for (const auto &ts_kv : profile.type_stats)
			types.push_back(ts_kv.first);
		std::sort(types.begin(), types.end());
		row.sorted_types = types;

		row.counts.reserve(types.size());
		row.mins.reserve(types.size());
		row.maxs.reserve(types.size());
		for (auto &t : types) {
			auto &stats = profile.type_stats.at(t);
			row.counts.push_back(stats.count);
			row.mins.push_back(stats.min_val);
			row.maxs.push_back(stats.max_val);
		}
		rows.push_back(std::move(row));
	}

	// Sort rows by predicate for deterministic output
	std::sort(rows.begin(), rows.end(),
	          [](const ProfileRow &a, const ProfileRow &b) { return a.predicate < b.predicate; });
	return rows;
}

// ============================================================
// Table function state
// ============================================================

struct ProfileRDFBindData : public TableFunctionData {
	vector<string> file_paths;
	ITriplesBuffer::FileType file_type = ITriplesBuffer::UNKNOWN;
	bool strict_parsing = true;
};

struct ProfileRDFGlobalState : public GlobalTableFunctionState {
	std::vector<ProfileRow> rows;
	std::atomic<idx_t> position {0};

	idx_t MaxThreads() const override {
		return 1; // output is small; single-thread emission is fine
	}
};

struct ProfileRDFLocalState : public LocalTableFunctionState {};

// ============================================================
// Bind
// ============================================================

static unique_ptr<FunctionData> ProfileRDFBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ProfileRDFBindData>();
	auto &fs = FileSystem::GetFileSystem(context);

	string pattern = input.inputs[0].GetValue<string>();
	auto glob_results = fs.Glob(pattern);
	if (glob_results.empty())
		throw IOException("No files found matching: " + pattern);
	for (auto &info : glob_results)
		result->file_paths.push_back(std::move(info.path));

	auto ft_it = input.named_parameters.find(PROFILE_FILE_TYPE);
	if (ft_it != input.named_parameters.end()) 
		result->file_type = ITriplesBuffer::ParseFileTypeString(ft_it->second.GetValue<string>());
	
	auto sp_it = input.named_parameters.find(PROFILE_STRICT_PARSING);
	if (sp_it != input.named_parameters.end())
		result->strict_parsing = sp_it->second.GetValue<bool>();

	names = {"predicate", "types", "count", "min", "max", "graph_count", "subject_count"};
	return_types = {
	    LogicalType::VARCHAR,                                         // predicate
	    LogicalType::LIST(LogicalType::VARCHAR),                      // types
	    LogicalType::MAP(LogicalType::VARCHAR, LogicalType::UBIGINT), // count
	    LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), // min
	    LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), // max
	    LogicalType::UBIGINT,                                         // graph_count
	    LogicalType::UBIGINT,                                         // subject_count
	};
	return std::move(result);
}

// ============================================================
// Global init — all parsing happens here
// ============================================================

static unique_ptr<GlobalTableFunctionState> ProfileRDFGlobalInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = (ProfileRDFBindData &)*input.bind_data;
	auto &fs = FileSystem::GetFileSystem(context);

	RDFProfileAccumulator accumulator;

	for (auto &file_path : bind_data.file_paths) {
		ITriplesBuffer::FileType ft = bind_data.file_type;
		if (ft == ITriplesBuffer::UNKNOWN)
			ft = ITriplesBuffer::DetectFileTypeFromPath(file_path);

		try {
			switch (ft) {
			case ITriplesBuffer::TURTLE:
			case ITriplesBuffer::NTRIPLES:
			case ITriplesBuffer::NQUADS:
			case ITriplesBuffer::TRIG:
				ProfileFileSerd(file_path, fs, ft, bind_data.strict_parsing, accumulator);
				break;
			case ITriplesBuffer::XML:
				ProfileFileXML(file_path, fs, bind_data.strict_parsing, accumulator);
				break;
			default:
				throw IOException("Cannot determine file type for: " + file_path);
			}
		} catch (const std::runtime_error &re) {
			throw IOException(re.what());
		}
	}

	auto state = make_uniq<ProfileRDFGlobalState>();
	state->rows = BuildRows(accumulator);
	return state;
}

// ============================================================
// Local init
// ============================================================

static unique_ptr<LocalTableFunctionState> ProfileRDFLocalInit(ExecutionContext & /*context*/,
                                                               TableFunctionInitInput & /*input*/,
                                                               GlobalTableFunctionState * /*global*/) {
	return make_uniq<ProfileRDFLocalState>();
}

// ============================================================
// Scan — emit pre-computed rows
// ============================================================

static void ProfileRDFFunc(ClientContext & /*context*/, TableFunctionInput &input, DataChunk &output) {
	auto &global = (ProfileRDFGlobalState &)*input.global_state;

	idx_t out_idx = 0;
	const idx_t capacity = STANDARD_VECTOR_SIZE;

	while (out_idx < capacity) {
		idx_t row_idx = global.position.fetch_add(1, std::memory_order_relaxed);
		if (row_idx >= global.rows.size())
			break;

		const ProfileRow &row = global.rows[row_idx];

		// predicate
		output.SetValue(0, out_idx, Value(row.predicate));

		// types: LIST<VARCHAR>
		{
			duckdb::vector<Value> type_vals;
			type_vals.reserve(row.sorted_types.size());
			for (auto &t : row.sorted_types)
				type_vals.emplace_back(Value(t));
			output.SetValue(1, out_idx, Value::LIST(LogicalType::VARCHAR, type_vals));
		}

		// count: MAP<VARCHAR, UBIGINT>
		output.SetValue(2, out_idx, BuildVarcharBigintMap(row.sorted_types, row.counts));

		// min: MAP<VARCHAR, VARCHAR>
		output.SetValue(3, out_idx, BuildVarcharVarcharMap(row.sorted_types, row.mins));

		// max: MAP<VARCHAR, VARCHAR>
		output.SetValue(4, out_idx, BuildVarcharVarcharMap(row.sorted_types, row.maxs));

		// graph_count
		output.SetValue(5, out_idx, Value::UBIGINT(row.graph_count));

		// subject_count
		output.SetValue(6, out_idx, Value::UBIGINT(row.subject_count));

		out_idx++;
	}

	output.SetCardinality(out_idx);
}

// ============================================================
// Registration
// ============================================================

void RegisterProfileRDF(ExtensionLoader &loader) {
	TableFunction tf("profile_rdf", {LogicalType::VARCHAR}, ProfileRDFFunc, ProfileRDFBind, ProfileRDFGlobalInit,
	                 ProfileRDFLocalInit);
	tf.named_parameters[PROFILE_FILE_TYPE] = LogicalType::VARCHAR;
	tf.named_parameters[PROFILE_STRICT_PARSING] = LogicalType::BOOLEAN;
	loader.RegisterFunction(tf);
}

} // namespace duckdb
