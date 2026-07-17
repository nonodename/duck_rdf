# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A DuckDB extension (extension name: `rdf`) for reading and writing RDF data directly in DuckDB. It registers the following functions (all registered in `LoadInternal` in `src/rdf_extension.cpp`; the full user-facing reference with parameters, return schemas, and examples is `docs/functions.md` — keep that file in sync when changing any function's signature or behavior):

| Function | Kind | What it does |
|----------|------|--------------|
| `read_rdf(path, ...)` | table function | Streams triples/quads from RDF files into a table |
| `read_rdf_prefixes(path, ...)` | table function | Returns `@prefix`/`@base` declarations from Turtle/TriG files |
| `profile_rdf(path, ...)` | table function | One row per unique predicate: observed types, counts, min/max, subject/graph counts |
| `pivot_rdf(path, ...)` | table function | Two-pass read producing a wide table: one row per subject, one typed column per predicate |
| `read_sparql(endpoint, query)` | table function | Runs a SPARQL SELECT against a remote HTTP(S) endpoint (native builds only) |
| `is_valid_r2rml(path)` | scalar function | Validates an R2RML/YARRRML mapping file |
| `can_call_inside_out(path)` | scalar function | True if a mapping can be used in inside-out COPY mode |
| `COPY ... TO ... (FORMAT r2rml, mapping '...')` | copy function | Writes query results to RDF via an R2RML or YARRRML mapping |

All four file-reading table functions accept a single path, a glob pattern, or a `LIST(VARCHAR)` of paths/globs, and transparently read `.gz` / `.zst` compressed files (decompression is auto-detected via DuckDB's FileSystem).

### WebAssembly build limitations

The WASM build (`make wasm_eh` / `make wasm_mvp`) excludes two features that require native OS capabilities:

| Feature | Reason excluded from WASM |
|---------|--------------------------|
| RDF/XML parsing (`file_type = 'rdf'/'xml'`) | Requires LibXml2, which is not compiled for WASM |
| `read_sparql()` | Requires libcurl, which has no WASM-compatible build (no OS sockets) |

The source files `src/rdf_xml_parser.cpp`, `src/xml_buffer.cpp`, and `src/sparql_reader.cpp` are only compiled for native targets (see the `if (NOT EMSCRIPTEN)` blocks in `CMakeLists.txt`). For Emscripten targets the preprocessor flags `DUCK_RDF_NO_XML` and `DUCK_RDF_NO_SPARQL` are defined and gate the relevant code paths so a clear error is raised if they are reached at runtime. The `test/wasm-smoke-test/` harness (Node.js, see its README) validates local Turtle/NTriples/NQuads/TriG reads only.

## Build & Test Commands

Native builds need vcpkg for libxml2/curl: `VCPKG_TOOLCHAIN_PATH` must point to `<vcpkg>/scripts/buildsystems/vcpkg.cmake` (see "Building" in the root README for the exact clone/bootstrap steps). Without it, CMake configure fails at `find_package(LibXml2/CURL)`.

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

# Run the code formatter (do this before committing C++ changes)
make format

# Static analysis
make tidy-check
```

The real Makefile logic lives in `extension-ci-tools/makefiles/duckdb_extension.Makefile` (a submodule); the root `Makefile` only sets `EXT_NAME`, pins MSVC for the `windows_amd64` platform, and includes it.

After building, `./build/release/duckdb` is a DuckDB shell with the extension pre-loaded.

### Running a Single Test

SQLLogicTests are `.test` files — there's no direct single-file runner exposed by `make`. To run one test from the repo root:

```bash
./build/release/test/unittest "test/sql/rdf.test"
```

Or load the extension and run queries interactively:

```bash
./build/release/duckdb
```

Any test that involves filenames needs to account for the fact that directory paths are different on Windows than unix based systems. Simplest solution is to wrap the filename column, e.g. `replace(filename,'\','/')`.

## Architecture

You must stick with C++11 — that's the standard DuckDB uses. Follow the DuckDB C++ style rules in `CONTRIBUTING.md` (no raw `new`/`delete`, prefer `unique_ptr`, use `idx_t` for counts/offsets, no `using namespace std` in headers, etc.).

### File Resolution (multi-file support)

`src/rdf_multi_file.cpp` provides two helpers shared by `read_rdf`, `read_rdf_prefixes`, `profile_rdf`, and `pivot_rdf`:
- `ResolveRDFFiles(context, input, function_name)` — expands the positional argument (a VARCHAR glob or a `LIST(VARCHAR)` of paths/globs) into a concrete file list via DuckDB's own MultiFileReader, with consistent "no files found" errors.
- `RegisterRDFFileListFunction(tf)` — registers a table function under both a `VARCHAR` and a `LIST(VARCHAR)` overload of its first argument.

When adding a new file-reading table function, use both helpers rather than re-implementing glob/list handling.

### Parser Interface

All parsers implement `ITriplesBuffer` (`src/include/I_triples_buffer.hpp`):
- Constructor takes `(path, base_uri, strict_parsing, expand_prefixes)`
- `StartParse()` — open the file and begin streaming (no arguments; configuration comes from the constructor)
- `PopulateChunk(DataChunk &output)` — fill a DataChunk; an empty chunk signals this buffer is exhausted (there is no separate `HasMore()` method)
- `SetColumnIds(col_ids)` — projection pushdown: maps the 6 logical columns to output slots (`-1` = not requested, don't populate)
- `SetFilters(filters, col_ids)` — filter pushdown: stores per-column `TableFilter`s (constant comparisons, `IS [NOT] NULL`, and AND-combinations; anything else is left for DuckDB to re-check). Filter evaluation lives in `src/table_filter_eval.cpp`
- `GetSkipCount()` — number of malformed rows skipped when `strict_parsing = false` (logged as a DuckDB warning per file)
- `SetProgressCounter(...)` — wires up the shared atomic byte counter used for progress reporting

`ITriplesBuffer` also owns the static file-type helpers: `ConvertLabelToFileType`, `DetectFileTypeFromPath`, `ParseFileTypeString`, `IsCompressedPath` (`.gz`/`.zst` — detection looks at the extension *under* the compression suffix, e.g. `data.nt.gz` → NTriples).

Concrete implementations:
- **SerdBuffer** (`src/serd_buffer.cpp`) — Turtle, NTriples, NQuads, TriG via the SERD library
- **SerdRangeBuffer** (`src/serd_range_buffer.cpp`) — subclass of SerdBuffer bounded to a byte range of one file; used for parallel scanning of large NTriples/NQuads files (see Threading Model)
- **XMLBuffer** (`src/xml_buffer.cpp`) + **RDFXMLParser** (`src/rdf_xml_parser.cpp`) — RDF/XML via LibXML2 (native builds only; excluded from WASM via `DUCK_RDF_NO_XML`)

Parser construction goes through the factory in `src/rdf_triples_factory.cpp`: `OpenTriplesFile(...)` (whole file, any format) and `OpenTriplesFileRange(...)` (byte range; NTriples/NQuads only — throws `InternalException` for other formats).

### Output Schema

`read_rdf` returns 6 VARCHAR columns: `graph`, `subject`, `predicate`, `object`, `object_datatype`, `object_lang` — plus an optional 7th VARCHAR column `filename` when called with `filename = true`. The `filename` column is synthetic: it is unknown to `ITriplesBuffer` and is populated (and filtered) in `RDFReaderFunc` in `src/rdf_extension.cpp`, where a filename filter can skip parsing entire non-matching files.

Both projection pushdown and filter pushdown are enabled (`tf.projection_pushdown = true`, `tf.filter_pushdown = true`): unused columns are not populated, and simple filters are applied inside the parsers before rows are emitted. Note DuckDB re-keys `TableFilterSet` by position within `column_ids`, so both `SetColumnIds` and `SetFilters` translate back to absolute column indices.

### Threading Model

`read_rdf` uses DuckDB's parallel table function API with two kinds of work items (see `RDFReaderGlobalState` / `RDFReaderLocalState` in `src/rdf_extension.cpp`):

- **Byte ranges** — large (≥ 2 × 64 MiB target range size, `RDF_TARGET_RANGE_BYTES`), uncompressed, seekable NTriples/NQuads files are split into byte ranges, each scanned independently by a `SerdRangeBuffer`. Safe because NT/NQ are one-statement-per-line with no cross-statement state. Disable with the `parallel_scan = false` named parameter.
- **Whole files** — everything else (Turtle, TriG, RDF/XML, compressed files, small files) is scanned one file per work item, as before.

Each thread claims the next work item under a mutex (ranges first, then whole files). The bind phase stats every matched file once; those sizes feed a cardinality estimate (`RDFReaderCardinality`, rough bytes-per-triple heuristics) and a progress bar (`RDFReaderProgress`, via the shared atomic `bytes_consumed` counter).

### Profiling and Pivoting

- `src/rdf_profiler.cpp` holds the shared per-predicate statistics engine; `src/profile_rdf.cpp` exposes it as the `profile_rdf` table function. Object types are reported as DuckDB type names mapped from XSD datatypes — the mapping is documented in `docs/typemapping.md`.
- `src/pivot_rdf.cpp` implements `pivot_rdf`: pass 1 profiles the input to derive the output schema (column per predicate; type rules for lang-tagged strings, mixed types → UNION, multi-valued → LIST), pass 2 re-reads and fills rows. Subjects repeated across files produce multiple rows by design.

### R2RML Write

`src/r2rml_copy.cpp` integrates the `sql2rdf` library (fetched via CMake FetchContent) and registers the `r2rml` copy format plus the `is_valid_r2rml` / `can_call_inside_out` scalar functions. Mappings can be R2RML Turtle (`.ttl`) or YARRRML YAML (`.yml`/`.yaml`/`.yarrrml`, parsed via yaml-cpp). Two execution modes:
- **Inside-out**: DuckDB drives the SQL query; each output row is handed to the extension for mapping → SERD output. Requires a mapping without `rr:logicalTable` (check with `can_call_inside_out`).
- **Full R2RML**: The extension executes the SQL queries named in the mapping file itself (streamed through `StreamingSQLResultSet`, which wraps a live DuckDB `QueryResult`), then maps and writes. The `COPY` source query is ignored — convention is `COPY (SELECT 1) TO ...`.

Copy options: `mapping` (required), `rdf_format` (`ntriples` default, `turtle`, `nquads`), `ignore_non_fatal_errors`, `ignore_case`. SERD is used for all RDF output.

### SPARQL Reader

`src/sparql_reader.cpp` uses CURL for HTTP GET, parses the SPARQL results CSV, and caches the full result set at bind (plan) time — large results are held in memory. Columns are VARCHAR, named after the SPARQL SELECT variables. Anonymous endpoints only. Not compiled for Emscripten targets (gated by `DUCK_RDF_NO_SPARQL`).

### Prefix Introspection

`src/read_rdf_prefixes.cpp` implements `read_rdf_prefixes`, returning `prefix`, `uri`, `is_base` (+ optional `filename`) rows from Turtle/TriG files. It errors for NTriples/NQuads/RDF-XML, which have no prefix declarations.

### Not part of the build

`src/CMakeLists-BufferTestHarness.txt` is a standalone ASAN development harness for SerdBuffer. It is not referenced by the extension build — ignore it unless specifically working on that harness.

## Key Dependencies

Git submodules (branch `v1.5-variegata`, i.e. the DuckDB v1.5 line — see `docs/UPDATING.md` for the version-bump procedure):
- `duckdb/` — DuckDB source the extension builds against
- `extension-ci-tools/` — shared DuckDB extension build framework (owns the real Makefile logic)

CMake FetchContent (fetched at configure time, not checked into the repo; pins live in `CMakeLists.txt`):
- `serd` v0.32.10 — RDF parsing/writing library (built as an internal static lib from an explicit source list; the target name must remain `serd` so sql2rdf reuses it)
- `sql2rdf` v2.0.0 — R2RML mapping compiler/evaluator
- `yaml-cpp` 0.9.0 — YARRRML parsing support

vcpkg (`vcpkg.json`): `libxml2`, `curl` (with SSL) — both excluded on the `emscripten` platform.

## Tests

SQLLogicTest `.test` files live in `test/sql/`; test data lives in `test/rdf/` (RDF fixtures), `test/r2rml/` (mapping files), and `test/xmlrdf/` (RDF/XML fixtures). Format:

```
# name: test/sql/rdf.test
# group: [sql]

require rdf

query I
SELECT COUNT(*) FROM read_rdf('path/to/file.nt');
----
9
```

Conventions:
- Every feature area has its own `.test` file (e.g. `filter_pushdown.test`, `projection_pushdown.test`, `read_rdf_parallel_ranges.test`, `gz_compression.test`, `zst_compression.test`, `pivot_rdf.test`, `write_rdf.test`). Add new tests alongside the feature they cover.
- The `read_sparql` tests (`test/sql/read_sparql*.test`) require internet access and may be skipped in CI.
- The WASM smoke test (`test/wasm-smoke-test/`) is a separate Node.js harness, not run by `make test`.
