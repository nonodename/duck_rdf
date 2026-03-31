
#ifndef I_TRIPLES_BUFFER_H
#define I_TRIPLES_BUFFER_H
#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>
#include <queue>
#include "string_util.hpp"
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
	uint64_t GetSkipCount() const {
		return _skip_count;
	}

	static ITriplesBuffer::FileType ConvertLabelToFileType(const std::string &s) {
		std::string x = stringtoLower(s);
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
};

#endif // I_TRIPLES_BUFFER_H
