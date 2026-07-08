#include "include/serd_range_buffer.hpp"
#include <algorithm>
#include <vector>

static const size_t RANGE_READ_BUFFER_SIZE = 1024 * 1024;

SerdRangeBuffer::SerdRangeBuffer(std::string path, std::string base_uri, duckdb::FileSystem *fs, bool strict_parsing,
                                 bool expand_prefixes, ITriplesBuffer::FileType file_type, uint64_t range_start,
                                 uint64_t range_end, uint64_t file_size)
    : SerdBuffer(std::move(path), std::move(base_uri), fs, strict_parsing, expand_prefixes, file_type),
      _range_start(range_start), _range_end(range_end), _read_pos(0) {
	uint64_t overrun_end = _range_end + SerdRangeBuffer::kMaxOverrunBytes;
	_hard_stop = std::min<uint64_t>(overrun_end, file_size);
}

void SerdRangeBuffer::AlignToRangeStart() {
	if (_range_start == 0) {
		_read_pos = 0;
		return;
	}

	_file_handle->Seek(_range_start);
	uint64_t pos = _range_start;
	std::vector<char> buf(RANGE_READ_BUFFER_SIZE);

	// Scan forward for the first '\n' at or after the range's nominal start;
	// parsing begins on the byte right after it. Every range independently
	// re-derives this same rule from its own nominal start, so adjacent
	// ranges converge on identical boundaries without any coordination.
	while (pos < _hard_stop) {
		idx_t to_read = (idx_t)std::min<uint64_t>(buf.size(), _hard_stop - pos);
		int64_t n = _file_handle->Read(buf.data(), to_read);
		if (n <= 0) {
			break; // Hit EOF (or our own hard stop) before finding a newline.
		}
		for (int64_t i = 0; i < n; i++) {
			if (buf[(size_t)i] == '\n') {
				uint64_t real_start = pos + (uint64_t)i + 1;
				_file_handle->Seek(real_start);
				_read_pos = real_start;
				return;
			}
		}
		pos += (uint64_t)n;
	}

	// No newline found before the hard stop (e.g. a single statement longer
	// than the whole range): this range legitimately produces zero rows.
	_file_handle->Seek(pos);
	_read_pos = pos;
}

void SerdRangeBuffer::StartParse() {
	AlignToRangeStart();

	// Bridge from SerdSource to DuckDB FileHandle. Once we've read past
	// _range_end, we keep consuming only up to (and including) the very next
	// '\n' - finishing the statement that was in flight at the boundary -
	// then report a clean end-of-stream. _hard_stop is just a safety cap for
	// a pathological statement longer than kMaxOverrunBytes, in which case
	// the read is cut off there instead, mid-statement (rare, acceptable
	// failure: existing skip-count/error handling takes over from there).
	auto duckdb_source = [](void *buf, size_t size, size_t nmemb, void *stream) -> size_t {
		auto self = static_cast<SerdRangeBuffer *>(stream);
		if (!self || !self->_file_handle || self->_finished) {
			return 0;
		}
		if (self->_read_pos >= self->_hard_stop) {
			self->_finished = true;
			return 0;
		}
		uint64_t remaining = self->_hard_stop - self->_read_pos;
		idx_t to_read = (idx_t)std::min<uint64_t>(nmemb, remaining);
		int64_t read = self->_file_handle->Read(buf, to_read);
		if (read <= 0) {
			self->_finished = true;
			return 0;
		}

		uint64_t prev_pos = self->_read_pos;
		self->_read_pos += (uint64_t)read;
		if (self->_progress_counter) {
			self->_progress_counter->fetch_add((uint64_t)read, std::memory_order_relaxed);
		}

		if (self->_read_pos > self->_range_end) {
			// We've crossed the nominal range end somewhere in this chunk;
			// scan only the portion at/after that point for the terminating
			// newline, and truncate the return value there.
			uint64_t scan_from = prev_pos >= self->_range_end ? 0 : (self->_range_end - prev_pos);
			auto *bytes = static_cast<char *>(buf);
			for (uint64_t i = scan_from; i < (uint64_t)read; i++) {
				if (bytes[i] == '\n') {
					self->_finished = true;
					return (size_t)(i + 1);
				}
			}
			// No newline yet in this chunk; if we've now hit the hard stop,
			// give up and hand back everything read so far.
			if (self->_read_pos >= self->_hard_stop) {
				self->_finished = true;
			}
		}
		return (size_t)read;
	};
	auto duckdb_error = [](void * /*stream*/) -> int {
		return 0;
	};

	serd_reader_start_source_stream(_reader.get(), (SerdSource)duckdb_source, (SerdStreamErrorFunc)duckdb_error, this,
	                                (uint8_t *)_file_path.c_str(), RANGE_READ_BUFFER_SIZE);
}

bool SerdRangeBuffer::AtStreamEnd() {
	return _finished || _read_pos >= _hard_stop;
}
