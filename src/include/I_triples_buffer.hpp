
#ifndef I_TRIPLES_BUFFER_H
#define I_TRIPLES_BUFFER_H
#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "table_filter_eval.hpp"
#include <algorithm>
#include <atomic>
#include <queue>
/*
    Holder for a single row of RDF
*/
struct RDFRow {
	std::string graph;
	std::string subject;
	std::string predicate;
	std::string object;
	std::string datatype;
	std::string lang;
};

class ITriplesBuffer {
public:
	// Supported file type hints for parsing
	enum FileType { TURTLE = 0, NQUADS, NTRIPLES, TRIG, XML, UNKNOWN };

	ITriplesBuffer(std::string path, std::string base_uri, bool strict_parsing = true,
	               const bool expand_prefixes = false)
	    : _base_uri(std::move(base_uri)), _file_path(std::move(path)) {};

	virtual void PopulateChunk(duckdb::DataChunk &output) = 0;
	virtual void StartParse() = 0;
	virtual ~ITriplesBuffer() = default;

	// Maps original column indices → output DataChunk slot (-1 = skip).
	// Default {0,1,2,3,4,5} is the identity (all 6 columns present).
	int8_t _output_slot[6] = {0, 1, 2, 3, 4, 5};

	void SetColumnIds(const duckdb::vector<duckdb::column_t> &col_ids) {
		std::fill(_output_slot, _output_slot + 6, (int8_t)-1);
		for (duckdb::idx_t i = 0; i < col_ids.size(); i++) {
			if (col_ids[i] < 6)
				_output_slot[col_ids[i]] = (int8_t)i;
		}
	}

	// Row filters pushed down from DuckDB for graph/subject/predicate/object/
	// object_datatype/object_lang (columns 0-5), compiled once per file/range
	// (see CompileColumnFilter) and reused for every row.
	CompiledColumnFilter _column_filters[6];

	// `filters` is keyed by *position within col_ids* (DuckDB re-keys
	// TableFilterSet relative to the projection list before handing it to the
	// table function - see CreateTableFilterSet in plan_get.cpp), so it must be
	// translated back to absolute column indices via col_ids, same as
	// SetColumnIds does above.
	void SetFilters(duckdb::ClientContext &context, duckdb::optional_ptr<duckdb::TableFilterSet> filters,
	                const duckdb::vector<duckdb::column_t> &col_ids) {
		if (!filters) {
			return;
		}
		for (auto &entry : *filters) {
			if (entry.GetIndex() >= col_ids.size()) {
				continue;
			}
			duckdb::column_t abs_col = col_ids[entry.GetIndex()];
			if (abs_col < 6) {
				_column_filters[abs_col] = CompileColumnFilter(context, &entry.Filter());
			}
		}
	}
	uint64_t GetSkipCount() const {
		return _skip_count;
	}

	void SetProgressCounter(std::atomic<uint64_t> *c) {
		_progress_counter = c;
	}

	static ITriplesBuffer::FileType ConvertLabelToFileType(const std::string &s) {
		// could we do a lower here and then test for that?
		// yeah, we could but who's naming files 'Ttl' or 'Nq'
		// so let's not bother with a lower() here.
		if (s == "ttl" || s == "turtle" || s == "TTL" || s == "TURTLE")
			return ITriplesBuffer::TURTLE;
		if (s == "nq" || s == "nquads" || s == "NQ" || s == "NQUADS")
			return ITriplesBuffer::NQUADS;
		if (s == "nt" || s == "ntriples" || s == "NT" || s == "NTRIPLES")
			return ITriplesBuffer::NTRIPLES;
		if (s == "trig" || s == "TRIG")
			return ITriplesBuffer::TRIG;
		if (s == "rdf" || s == "xml" || s == "RDF" || s == "XML")
			return ITriplesBuffer::XML;
		return ITriplesBuffer::UNKNOWN;
	}

	static bool IsCompressedPath(const std::string &path) {
		auto pos = path.rfind('.');
		if (pos == std::string::npos)
			return false;
		auto ext = path.substr(pos + 1);
		return ext == "gz" || ext == "zst" || ext == "GZ" || ext == "ZST";
	}

	static ITriplesBuffer::FileType DetectFileTypeFromPath(const std::string &path) {
		std::string inner = IsCompressedPath(path) ? path.substr(0, path.rfind('.')) : path;
		auto pos = inner.rfind('.');
		if (pos == std::string::npos)
			return ITriplesBuffer::UNKNOWN;
		return ConvertLabelToFileType(inner.substr(pos + 1));
	}

	static ITriplesBuffer::FileType ParseFileTypeString(const std::string &s) {
		ITriplesBuffer::FileType ft = ConvertLabelToFileType(s);
		if (ft == ITriplesBuffer::UNKNOWN)
			throw duckdb::InvalidInputException("Unknown file_type override: '%s'", s.c_str());
		return ft;
	}

protected:
	// Use DuckDB FileSystem and FileHandle for reading files (allows remote filesystems)
	duckdb::FileSystem *_fs = nullptr;
	std::unique_ptr<duckdb::FileHandle> _file_handle;
	std::string _base_uri;
	std::string _file_path;
	uint64_t _skip_count = 0;

	duckdb::DataChunk *_current_chunk = nullptr;
	duckdb::idx_t _current_count = 0;
	std::deque<RDFRow> _overflow_buffer;
	bool _eof = false;
	bool _strict_parsing = true;
	bool _expand_prefixes = false;
	std::atomic<uint64_t> *_progress_counter = nullptr;
};

#endif // I_TRIPLES_BUFFER_H
