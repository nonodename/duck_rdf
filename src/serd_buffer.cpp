
#include "include/serd_buffer.hpp"
#include "duckdb/common/exception.hpp"
#include <iostream>
#include <stdexcept>
#include <memory>

static SerdSyntax MapSyntaxFromFileType(ITriplesBuffer::FileType file_type) {
	switch (file_type) {
	case ITriplesBuffer::TURTLE:
		return SERD_TURTLE;
	case ITriplesBuffer::NQUADS:
		return SERD_NQUADS;
	case ITriplesBuffer::NTRIPLES:
		return SERD_NTRIPLES;
	case ITriplesBuffer::TRIG:
		return SERD_TRIG;
	default:
		throw std::runtime_error("Cannot detect SerdSyntax from unknown file type");
	}
}
/*
    SerdBuffer constructor. Using managed pointers for the SERD calls
*/
SerdBuffer::SerdBuffer(std::string path, std::string base_uri, duckdb::FileSystem *fs, const bool strict_parsing,
                       const bool expand_prefixes, const ITriplesBuffer::FileType file_type)
    : _reader(nullptr, &serd_reader_free), _env(nullptr, &serd_env_free),
      ITriplesBuffer(path, base_uri, strict_parsing, expand_prefixes) {
	if (!fs) {
		throw std::runtime_error("SerdBuffer requires a valid DuckDB FileSystem pointer");
	}
	// Assign base class FileSystem pointer and open via FileSystem to allow remote filesystems
	this->_fs = fs;
	try {
		this->_file_handle = this->_fs->OpenFile(this->_file_path, duckdb::FileFlags::FILE_FLAGS_READ);
	} catch (std::exception &ex) {
		throw std::runtime_error("Could not open RDF file: " + this->_file_path + ": " + ex.what());
	}
	SerdNode base = SERD_NODE_NULL;
	if (!base_uri.empty()) {
		base = serd_node_from_string(SERD_URI, (const uint8_t *)base_uri.c_str());
	}
	SerdEnv *t_env = serd_env_new(&base);
	if (!t_env) {
		throw std::runtime_error("Unable to create serd environment");
	}
	_env.reset(t_env);
	_strict_parsing = strict_parsing;
	SerdSyntax syntax = MapSyntaxFromFileType(file_type);

	if (syntax != SERD_NQUADS && syntax != SERD_NTRIPLES) {
		_expand_prefixes = expand_prefixes;
	} else { // Prefixes don't make sense in triple/quad formats
		_expand_prefixes = false;
	}

	SerdReader *t_reader =
	    serd_reader_new(syntax, this, nullptr, &BaseCallback, &PrefixCallback, &StatementCallback, nullptr);
	if (!t_reader) {
		throw std::runtime_error("Unable to create a serd reader for parsing");
	}
	serd_reader_set_strict(t_reader, strict_parsing);
	serd_reader_set_error_sink(t_reader, &ErrorCallBack, this);
	_reader.reset(t_reader);
}

SerdBuffer::~SerdBuffer() {
	if (_reader.get())
		serd_reader_end_stream(_reader.get());
	serd_reader_free(_reader.release());
	if (_env.get())
		serd_env_free(_env.release());
	_file_handle.reset();
}

/*
    Start parsing the RDF file. Only needs to be called once.
*/
void SerdBuffer::StartParse() {
	const char *fp = _file_path.c_str();

	// Bridge from SerdSource to DuckDB FileHandle
	auto duckdb_source = [](void *buf, size_t size, size_t nmemb, void *stream) -> size_t {
		// stream is a duckdb::FileHandle*
		auto fh = static_cast<duckdb::FileHandle *>(stream);
		if (!fh)
			return 0;
		int64_t read = fh->Read(buf, (idx_t)nmemb);
		return (size_t)std::max<int64_t>(read, 0);
	};
	auto duckdb_error = [](void * /*stream*/) -> int {
		return 0;
	};

	serd_reader_start_source_stream(_reader.get(), (SerdSource)duckdb_source, (SerdStreamErrorFunc)duckdb_error,
	                                _file_handle.get(), (uint8_t *)fp, 4096U);
}

void SerdBuffer::WriteToVector(duckdb::Vector &vec, idx_t row_idx, const SerdNode *node) {
	if (!node || !node->buf) {
		duckdb::FlatVector::SetNull(vec, row_idx, true);
		return;
	}
	// Zero-copy from Serd buffer to DuckDB String Heap
	if (_expand_prefixes && node->type == SERD_CURIE) {
		SerdNode expanded = serd_env_expand_node(_env.get(), node);
		if (expanded.buf) {
			auto str = duckdb::StringVector::AddString(vec, (const char *)expanded.buf, expanded.n_bytes);
			duckdb::FlatVector::GetData<duckdb::string_t>(vec)[row_idx] = str;
			serd_node_free(&expanded);
			return;
		}
		// If expansion failed, fall through to adding the original CURIE
	}
	auto str = duckdb::StringVector::AddString(vec, (const char *)node->buf, node->n_bytes);
	duckdb::FlatVector::GetData<duckdb::string_t>(vec)[row_idx] = str;
}

void SerdBuffer::PopulateChunk(duckdb::DataChunk &output) {
	_current_chunk = &output;
	_current_count = 0;

	// 1. Drain overflow buffer first (if any)
	while (!_overflow_buffer.empty() && _current_count < STANDARD_VECTOR_SIZE) {
		RDFRow row = _overflow_buffer.front();
		_overflow_buffer.pop_front();
		// Manual copy from string to vector (slow path)
		if (_output_slot[0] >= 0)
			output.SetValue(_output_slot[0], _current_count, duckdb::Value(row.graph));
		if (_output_slot[1] >= 0)
			output.SetValue(_output_slot[1], _current_count, duckdb::Value(row.subject));
		if (_output_slot[2] >= 0)
			output.SetValue(_output_slot[2], _current_count, duckdb::Value(row.predicate));
		if (_output_slot[3] >= 0)
			output.SetValue(_output_slot[3], _current_count, duckdb::Value(row.object));
		if (_output_slot[4] >= 0)
			output.SetValue(_output_slot[4], _current_count, duckdb::Value(row.datatype));
		if (_output_slot[5] >= 0)
			output.SetValue(_output_slot[5], _current_count, duckdb::Value(row.lang));
		_current_count++;
	}

	// 2. Parse from file directly into Chunk
	while (_current_count < STANDARD_VECTOR_SIZE && !_eof) {
		SerdStatus st = serd_reader_read_chunk(_reader.get());
		switch (st) {
		case SERD_SUCCESS:
			// Loop continues; Callback increments current_count
			_eof = false;
			break;

		case SERD_FAILURE:
			serd_reader_end_stream(_reader.get());
			// Determine EOF by comparing file position to file size
			try {
				idx_t pos = _file_handle->SeekPosition();
				int64_t sz = _fs->GetFileSize(*_file_handle);
				if (sz >= 0 && pos >= (idx_t)sz) {
					_eof = true;
				} else {
					if (_has_error) {
						throw duckdb::SyntaxException(_error_message);
					} else {
						throw std::runtime_error("SERD failure");
					}
				}
			} catch (std::exception &ex) {
				if (_has_error) {
					throw duckdb::SyntaxException(_error_message);
				} else {
					throw std::runtime_error(std::string("SERD failure: ") + ex.what());
				}
			}
			break;
		case SERD_ERR_BAD_CURIE:
		case SERD_ERR_ID_CLASH:
		case SERD_ERR_BAD_TEXT:
		case SERD_ERR_INTERNAL:
			throw std::runtime_error("SERD Error: " + SerdStatusToString(st));
		case SERD_ERR_BAD_SYNTAX:
			if (_strict_parsing) {
				if (_has_error) {
					throw duckdb::SyntaxException(_error_message);
				} else {
					throw duckdb::SyntaxException("SERD bad RDF syntax");
				}
			} else {
				if (serd_reader_skip_until_byte(_reader.get(), '\n') == SERD_FAILURE)
					throw std::runtime_error("SERD failure while skipping after syntax error");
			}
		default:
			throw std::runtime_error("SERD other error");
			break;
		}
	}

	output.SetCardinality(_current_count);
	_current_chunk = nullptr; // Clear pointer for safety
}

string SerdBuffer::SafeString(const SerdNode *node) {
	if (!node || !node->buf || node->n_bytes == 0)
		return {};
	std::string retVal;
	if (_expand_prefixes && node->type == SERD_CURIE) {
		SerdNode expanded = serd_env_expand_node(_env.get(), node);
		if (expanded.buf) {
			retVal = std::string(reinterpret_cast<const char *>(expanded.buf), expanded.n_bytes);
			serd_node_free(&expanded);
		} else {
			retVal = std::string(reinterpret_cast<const char *>(node->buf), node->n_bytes);
		}
	} else {
		retVal = std::string(reinterpret_cast<const char *>(node->buf), node->n_bytes);
	}
	return retVal;
}

string SerdBuffer::SerdStatusToString(SerdStatus status) {
	switch (status) {
	case SERD_SUCCESS:
		return "Success";
	case SERD_FAILURE:
		return "Non-fatal failure";
	case SERD_ERR_UNKNOWN:
		return "Unknown error";
	case SERD_ERR_BAD_SYNTAX:
		return "Invalid syntax";
	case SERD_ERR_BAD_ARG:
		return "Invalid argument";
	case SERD_ERR_NOT_FOUND:
		return "Not found";
	case SERD_ERR_ID_CLASH:
		return "ID clash";
	case SERD_ERR_BAD_CURIE:
		return "Bad CURIE";
	case SERD_ERR_INTERNAL:
		return "Internal error";
	case SERD_ERR_BAD_WRITE:
		return "Write error";
	case SERD_ERR_BAD_TEXT:
		return "Bad text encoding";
	default:
		return "Unrecognized SerdStatus";
	}
}

SerdStatus SerdBuffer::StatementCallback(void *user_data, SerdStatementFlags, const SerdNode *graph,
                                         const SerdNode *subject, const SerdNode *predicate, const SerdNode *object,
                                         const SerdNode *object_datatype, const SerdNode *object_lang) {
	auto *self = static_cast<SerdBuffer *>(user_data);

	// Safety check: If chunk is full, push to overflow and return
	if (self->_current_count >= STANDARD_VECTOR_SIZE) {
		RDFRow row;
		row.subject = self->SafeString(subject);
		row.predicate = self->SafeString(predicate);
		row.object = self->SafeString(object);
		row.graph = self->SafeString(graph);
		row.datatype = self->SafeString(object_datatype);
		row.lang = self->SafeString(object_lang);
		self->_overflow_buffer.push_back(std::move(row));
		return SERD_SUCCESS;
	}

	// Fast Path: Direct Write to DuckDB Vectors
	// Note: DataChunk columns map to: 0:graph, 1:subject, 2:predicate, 3:object, ...
	const int8_t *slots = self->_output_slot;
	if (slots[0] >= 0)
		self->WriteToVector(self->_current_chunk->data[slots[0]], self->_current_count, graph);
	if (slots[1] >= 0)
		self->WriteToVector(self->_current_chunk->data[slots[1]], self->_current_count, subject);
	if (slots[2] >= 0)
		self->WriteToVector(self->_current_chunk->data[slots[2]], self->_current_count, predicate);
	if (slots[3] >= 0)
		self->WriteToVector(self->_current_chunk->data[slots[3]], self->_current_count, object);
	if (slots[4] >= 0)
		self->WriteToVector(self->_current_chunk->data[slots[4]], self->_current_count, object_datatype);
	if (slots[5] >= 0)
		self->WriteToVector(self->_current_chunk->data[slots[5]], self->_current_count, object_lang);

	self->_current_count++;
	return SERD_SUCCESS;
}

// While SERD does provide a skip function for syntax errors (serd_reader_skip_until_byte)
// it doesn't seem like calling it actually helps.
SerdStatus SerdBuffer::ErrorCallBack(void *user_data, const SerdError *error) {
	auto *self = static_cast<SerdBuffer *>(user_data);
	if (self->_strict_parsing) {
		self->_has_error = true;
		self->_error_message = "SERD parsing error '" + SerdStatusToString(error->status) + "' in '" +
		                       self->_file_path + "', at line " + std::to_string(error->line) + ", column " +
		                       std::to_string(error->col);
		return SERD_FAILURE;
	} else {
		self->_skip_count++;
		return SERD_SUCCESS;
	}
}

SerdStatus SerdBuffer::BaseCallback(void *user_data, const SerdNode *uri) {
	auto *self = static_cast<SerdBuffer *>(user_data);
	serd_env_set_base_uri(self->_env.get(), uri);
	return SERD_SUCCESS;
}

SerdStatus SerdBuffer::PrefixCallback(void *user_data, const SerdNode *name, const SerdNode *uri) {
	auto *self = static_cast<SerdBuffer *>(user_data);
	if (self->_expand_prefixes) {
		// Update SerdEnv with new prefix mapping
		serd_env_set_prefix(self->_env.get(), name, uri);
	}
	return SERD_SUCCESS;
}
