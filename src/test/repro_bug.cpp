#include "repro_bug.hpp"
#include "duckdb/function/table_function.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>

namespace duckdb {

struct NineRowsBindData : public TableFunctionData {};

struct NineRowsGlobalState : public GlobalTableFunctionState {
	bool done = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

struct NineRowsLocalState : public LocalTableFunctionState {
	vector<column_t> column_ids;
};

static unique_ptr<FunctionData> NineRowsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"col_a", "col_b"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	return make_uniq<NineRowsBindData>();
}

static unique_ptr<GlobalTableFunctionState> NineRowsGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<NineRowsGlobalState>();
}

static unique_ptr<LocalTableFunctionState> NineRowsLocalInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state) {
	auto state = make_uniq<NineRowsLocalState>();
	state->column_ids = input.column_ids;
	return state;
}

static void NineRowsFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &gstate = (NineRowsGlobalState &)*input.global_state;
	auto &lstate = (NineRowsLocalState &)*input.local_state;
	if (gstate.done) {
		return; // empty chunk signals end of scan
	}
	gstate.done = true;

	// col_a is always non-null; col_b is always NULL - mirrors read_rdf's
	// always-NULL "graph" column for NTriples input.
	for (idx_t i = 0; i < lstate.column_ids.size(); i++) {
		auto &vec = output.data[i];
		if (lstate.column_ids[i] == 0) {
			for (idx_t row = 0; row < 9; row++) {
				auto s = StringVector::AddString(vec, "row" + std::to_string(row));
				FlatVector::GetDataMutable<string_t>(vec)[row] = s;
			}
		} else {
			for (idx_t row = 0; row < 9; row++) {
				FlatVector::SetNull(vec, row, true);
			}
		}
	}
	output.SetCardinality(9);
}

static void LoadReproBug(ExtensionLoader &loader) {
	TableFunction tf("nine_rows", {}, NineRowsFunc, NineRowsBind, NineRowsGlobalInit, NineRowsLocalInit);
	tf.projection_pushdown = true;
	CreateTableFunctionInfo info(tf);
	loader.RegisterFunction(std::move(info));
}

void RegisterReproBug(ExtensionLoader &loader) {
	LoadReproBug(loader);
}

} // namespace duckdb
