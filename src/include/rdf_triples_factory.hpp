#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "I_triples_buffer.hpp"

namespace duckdb {

// Opens a single RDF file and returns the appropriate ITriplesBuffer parser for
// its file type (auto-detected from the extension when ft == UNKNOWN). Shared by
// read_rdf and pivot_rdf, which both need to open individual matched files.
unique_ptr<ITriplesBuffer> OpenTriplesFile(const string &file_path, ITriplesBuffer::FileType ft, FileSystem &fs,
                                           bool strict_parsing, bool expand_prefixes);

// Opens a byte range [range_start, range_end) of a single NTriples/NQuads file
// for parallel scanning. Only NTRIPLES/NQUADS are supported (they have no
// cross-statement state, so a file can be safely split into independent
// ranges); any other file type throws InternalException, since callers are
// expected to have already gated eligibility before calling this. file_size
// is the full file's size, used to bound how far a range is allowed to read
// past range_end to finish an in-flight statement.
unique_ptr<ITriplesBuffer> OpenTriplesFileRange(const string &file_path, ITriplesBuffer::FileType ft, FileSystem &fs,
                                                bool strict_parsing, bool expand_prefixes, idx_t range_start,
                                                idx_t range_end, idx_t file_size);

} // namespace duckdb
