#ifndef SERD_RANGE_BUFFER_H
#define SERD_RANGE_BUFFER_H

#include "serd_buffer.hpp"

/*
    Byte-range-bounded variant of SerdBuffer, used for parallel scanning of a
    single large NTriples/NQuads file. NTriples/NQuads are one-statement-per-
    line formats with no cross-statement state (no prefixes; blank-node labels
    are file-scoped but are only ever emitted as opaque strings), so a file can
    be safely split into independent byte ranges, each parsed by its own
    SerdRangeBuffer/SerdReader instance.

    A range is [range_start, range_end). On construction the buffer seeks to
    range_start and scans forward to the next '\n' so it never begins parsing
    mid-statement (unless range_start == 0, which is trivially a boundary).
    While parsing, once it has read past range_end it keeps consuming only
    until the very next '\n' (finishing the statement that was in flight at
    the boundary), then reports a synthetic end-of-stream. kMaxOverrunBytes is
    just a safety cap for a pathological single statement longer than that,
    in which case the read is cut off there instead (rare, acceptable failure
    mode). Adjacent ranges independently derive the same alignment rule from
    their own nominal start, so no statement is ever duplicated or dropped
    across a boundary.
*/
class SerdRangeBuffer : public SerdBuffer {
public:
	// A single range is only useful if it's actually a strict sub-range worth
	// bounding; kMaxOverrunBytes must comfortably exceed the longest real
	// NTriples/NQuads line expected in practice.
	static constexpr uint64_t kMaxOverrunBytes = 4ULL * 1024 * 1024;

	SerdRangeBuffer(std::string path, std::string base_uri, duckdb::FileSystem *fs, bool strict_parsing,
	                bool expand_prefixes, ITriplesBuffer::FileType file_type, uint64_t range_start, uint64_t range_end,
	                uint64_t file_size);

	void StartParse() override;

protected:
	bool AtStreamEnd() override;

private:
	// Seeks to _range_start (if nonzero) and scans forward to the first '\n',
	// so parsing always begins on a statement boundary.
	void AlignToRangeStart();

	uint64_t _range_start;
	uint64_t _range_end;
	uint64_t _hard_stop;
	uint64_t _read_pos;
	bool _finished = false;
};

#endif // SERD_RANGE_BUFFER_H
