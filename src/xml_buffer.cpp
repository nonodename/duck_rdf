#include "include/xml_buffer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

XMLBuffer::XMLBuffer(std::string path, std::string base_uri, duckdb::FileSystem *fs, const bool strict_parsing,
                     const bool expand_prefixes, const ITriplesBuffer::FileType file_type)
    : ITriplesBuffer(path, base_uri, strict_parsing, expand_prefixes),
      _parser([this](const RdfStatement &s) { this->statementCallback(s); },
              [this](const std::string &prefix, const std::string &uri) { this->namespaceCallback(prefix, uri); },
              [this](const std::string &msg) { this->errorCallback(msg); }, base_uri) {
	if (!fs) {
		throw std::runtime_error("XMLBuffer requires a valid DuckDB FileSystem pointer");
	}
	this->_fs = fs;
	try {
		this->_file_handle = this->_fs->OpenFile(this->_file_path, duckdb::FileFlags::FILE_FLAGS_READ);
	} catch (std::exception &ex) {
		throw std::runtime_error("Could not open RDF file: " + this->_file_path + ": " + ex.what());
	}
	_parser.setBlankNodePrefix("genid");
}
XMLBuffer::~XMLBuffer() {
}

void XMLBuffer::PopulateChunk(duckdb::DataChunk &output) {
	_current_chunk = &output;
	_current_count = 0;
	while (!_overflow_buffer.empty() && _current_count < STANDARD_VECTOR_SIZE) {
		RDFRow row = _overflow_buffer.front();
		_overflow_buffer.pop_front();
		// Use writeToVector (fast path: StringVector::AddString + FlatVector::GetData).
		if (_output_slot[0] >= 0)
			writeToVector(output.data[_output_slot[0]], _current_count, row.graph);
		if (_output_slot[1] >= 0)
			writeToVector(output.data[_output_slot[1]], _current_count, row.subject);
		if (_output_slot[2] >= 0)
			writeToVector(output.data[_output_slot[2]], _current_count, row.predicate);
		if (_output_slot[3] >= 0)
			writeToVector(output.data[_output_slot[3]], _current_count, row.object);
		if (_output_slot[4] >= 0)
			writeToVector(output.data[_output_slot[4]], _current_count, row.datatype);
		if (_output_slot[5] >= 0)
			writeToVector(output.data[_output_slot[5]], _current_count, row.lang);
		_current_count++;
	}

	char buffer[PARSING_CHUNK_SIZE];
	while (_current_count < STANDARD_VECTOR_SIZE && !_eof) {
		// Read up to PARSING_CHUNK_SIZE bytes via DuckDB FileHandle
		int64_t res = _file_handle->Read(buffer, PARSING_CHUNK_SIZE);
		if (res < PARSING_CHUNK_SIZE) {
			_eof = true;
		}
		_parser.parseChunk(buffer, (int)res, _eof);
		// Re-throw any exception that was deferred from within a SAX callback.
		if (_deferred_error) {
			throw duckdb::SyntaxException(_deferred_error_message);
		}
	}
	output.SetCardinality(_current_count);
	_current_chunk = nullptr;
}
void XMLBuffer::StartParse() {
}

void XMLBuffer::writeToVector(duckdb::Vector &vec, idx_t row_idx, const std::string &field) {
	if (field.empty()) {
		duckdb::FlatVector::SetNull(vec, row_idx, true);
		return;
	}

	auto str = duckdb::StringVector::AddString(vec, field);
	duckdb::FlatVector::GetData<duckdb::string_t>(vec)[row_idx] = str;
}
void XMLBuffer::statementCallback(const RdfStatement &stmt) {
	// This callback is invoked from within libxml2 SAX handlers (C code on the
	// call stack). Throwing a C++ exception through C frames is undefined
	// behaviour, so we use a deferred-error pattern: catch any exception, store
	// it, and re-throw it after xmlParseChunk() returns in PopulateChunk().
	try {
		// Safety check: If chunk is full, push to overflow and return
		if (_current_count >= STANDARD_VECTOR_SIZE) {
			RDFRow row;
			row.subject = stmt.subject;
			row.predicate = stmt.predicate;
			row.object = stmt.object;
			row.graph = "";
			row.datatype = stmt.datatype;
			row.lang = stmt.language;
			_overflow_buffer.push_back(std::move(row));
			return;
		}
		// Fast path with slot mapping
		const int8_t *slots = _output_slot;
		if (slots[0] >= 0)
			writeToVector(_current_chunk->data[slots[0]], _current_count, "");
		if (slots[1] >= 0)
			writeToVector(_current_chunk->data[slots[1]], _current_count, stmt.subject);
		if (slots[2] >= 0)
			writeToVector(_current_chunk->data[slots[2]], _current_count, stmt.predicate);
		if (slots[3] >= 0)
			writeToVector(_current_chunk->data[slots[3]], _current_count, stmt.object);
		if (slots[4] >= 0)
			writeToVector(_current_chunk->data[slots[4]], _current_count, stmt.datatype);
		if (slots[5] >= 0)
			writeToVector(_current_chunk->data[slots[5]], _current_count, stmt.language);
		_current_count++;
	} catch (const std::exception &ex) {
		if (!_deferred_error) {
			_deferred_error = true;
			_deferred_error_message = ex.what();
			_eof = true; // stop issuing further parseChunk calls
		}
	}
}

void XMLBuffer::namespaceCallback(const std::string &prefix, const std::string &uri) {
	// Also called from within a SAX handler — use deferred error pattern.
	try {
		_parser.addNameSpace(prefix, uri);
	} catch (const std::exception &ex) {
		if (!_deferred_error) {
			_deferred_error = true;
			_deferred_error_message = ex.what();
			_eof = true;
		}
	}
}

void XMLBuffer::errorCallback(const std::string &msg) {
	// on_error is called from RdfXmlParser::parseChunk() AFTER xmlParseChunk()
	// returns, so it is safe to throw directly here.
	throw duckdb::SyntaxException("Error in '" + _file_path + "': " + msg);
}
