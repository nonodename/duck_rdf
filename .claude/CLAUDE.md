# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A DuckDB extension that enables reading and writing RDF data directly in DuckDB. It provides:
- `read_rdf(path, options)` — read RDF files into DuckDB tables (glob patterns supported)
- `read_sparql(endpoint, query)` — query remote SPARQL endpoints (native builds only; not available in WASM)
- `profile_rdf(path, options)` - profile RDF files generating an DuckDB table overview
- `COPY TO ... (FORMAT r2rml)` — write DuckDB query results to RDF using R2RML mappings

### WebAssembly build limitations

The WASM build (`make wasm_eh` / `make wasm_mvp`) excludes two features that require native OS capabilities:

| Feature | Reason excluded from WASM |
|---------|--------------------------|
| RDF/XML parsing (`file_type = 'rdf'/'xml'`) | Requires LibXml2, which is not compiled for WASM |
| `read_sparql()` | Requires libcurl, which has no WASM-compatible build (no OS sockets) |

The source files `src/rdf_xml_parser.cpp`, `src/xml_buffer.cpp`, and `src/sparql_reader.cpp` are not compiled for Emscripten targets. The preprocessor flags `DUCK_RDF_NO_XML` and `DUCK_RDF_NO_SPARQL` gate the relevant code paths so a clear error is raised if those code paths are reached at runtime (they won't be for normal WASM use). The WASM `test/wasm-smoke-test/` harness validates local Turtle/NTriples/NQuads/TriG reads only.

## Build & Test Commands

```bash
# First-time setup
git submodule update --init --recursive

# Build (release)
make

# Build (faster, requires ninja)
GEN=ninja make

# Run all tests
make test

# Run tests against debug build
make test_debug

# Format check (run before a commit)
make format

# Static analysis
make tidy-check
```

After building, `./build/release/duckdb` is a DuckDB shell with the extension pre-loaded.

### Running a Single Test

SQLLogicTests are `.test` files — there's no direct single-file runner exposed by `make`. To run one test:

```bash
./build/release/test/unittest "test/sql/rdf.test"
```

Or load the extension and run queries interactively:

```bash
./build/release/duckdb
```

Any test that involves filenames needs to account for the fact that directory paths are different on Windows than unix based systems. Simplest solution is to wrap the filename column. Something like `replace(filename,'\','/')`

## Architecture

Note that you must stick with C++ 11 and earlier as that's the standard that DuckDB uses.

### Parser Interface

All parsers implement `ITriplesBuffer` ([src/include/I_triples_buffer.hpp](src/include/I_triples_buffer.hpp)):
- `StartParse(filename, options)` — open and begin streaming a file
- `PopulateChunk(chunk, ids)` — fill a DuckDB DataChunk (only requested column IDs)
- `HasMore()` — signal more rows available

Concrete implementations:
- **SerdBuffer** ([src/serd_buffer.cpp](src/serd_buffer.cpp)) — Turtle, NTriples, NQuads, TriG via the SERD library
- **XMLBuffer** ([src/xml_buffer.cpp](src/xml_buffer.cpp)) + **RDFXMLParser** ([src/rdf_xml_parser.cpp](src/rdf_xml_parser.cpp)) — RDF/XML via LibXML2 (native builds only; excluded from WASM via `DUCK_RDF_NO_XML`)

### Output Schema

`read_rdf` always returns exactly 6 VARCHAR columns: `graph`, `subject`, `predicate`, `object`, `object_datatype`, `object_lang`. Projection pushdown is enabled — unused columns are not populated.

### Threading Model

`read_rdf` uses DuckDB's parallel table function API. Each thread claims the next unread file (mutex-protected counter), creates the appropriate parser, and streams triples into DataChunks. See `RdfGlobalState` / `RdfLocalState` in [src/rdf_extension.cpp](src/rdf_extension.cpp).

### R2RML Write

[src/r2rml_copy.cpp](src/r2rml_copy.cpp) integrates the `sql2rdf` library (fetched via CMake FetchContent). Two execution modes:
- **Inside-out**: DuckDB drives the SQL query; each output row is handed to the extension for mapping → SERD output
- **Full R2RML**: The extension executes the SQL queries itself from the mapping file, caches results, then maps and writes

SERD is used for all RDF output (NTriples default, Turtle, NQuads).

### SPARQL Reader

[src/sparql_reader.cpp](src/sparql_reader.cpp) uses CURL for HTTP GET, parses SPARQL results CSV, caches the full result at plan time. Columns are VARCHAR, named after SPARQL variables. Not compiled for Emscripten targets (gated by `DUCK_RDF_NO_SPARQL`).

## Key Dependencies

Git submodules:
- `duckdb/` — DuckDB v1.5.0 source (builds the extension against this version)
- `extension-ci-tools/` — shared DuckDB extension build framework (owns the real Makefile logic)

CMake FetchContent (fetched at configure time, not checked into the repo):
- `serd` — RDF parsing/writing library
- `sql2rdf` — R2RML mapping compiler/evaluator
- `yaml-cpp` — YARRRML parsing support

vcpkg: `libxml2`, `curl` (with SSL)

## Tests

Tests live in `test/sql/` as SQLLogicTest `.test` files. Format:

```
# name: test/sql/rdf.test
# group: [sql]

require rdf

query I
SELECT COUNT(*) FROM read_rdf('path/to/file.nt');
----
9
```

The `read_sparql` tests (`test/sql/read_sparql*.test`) require internet access and may be skipped in CI.
