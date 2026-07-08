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

} // namespace duckdb
