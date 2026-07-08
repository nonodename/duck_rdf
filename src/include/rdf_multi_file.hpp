#pragma once

#include "duckdb.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Resolves the positional file-path argument (a VARCHAR glob pattern or a
// LIST[VARCHAR] of paths/globs) into a concrete, expanded list of files.
// Shared by read_rdf, pivot_rdf, profile_rdf, and read_rdf_prefixes so glob
// expansion, list-of-files parsing, and "no files found" errors are handled
// consistently via DuckDB's own MultiFileReader.
vector<OpenFileInfo> ResolveRDFFiles(ClientContext &context, TableFunctionBindInput &input,
                                     const string &function_name);

// Registers a table function under both a VARCHAR and a LIST(VARCHAR) overload
// of its first (file path) argument, sharing the same bind/init/scan functions.
TableFunctionSet RegisterRDFFileListFunction(TableFunction tf);

} // namespace duckdb
