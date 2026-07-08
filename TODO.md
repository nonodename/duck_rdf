## Future work

Potential future enhancements, courtesy Claude, below. If any of these seem interesting to you please indicate via an issue (or implement as a pull request :-)).

## Functional Enhancements
1. **Typed object values**
Currently all 6 columns are VARCHAR. The object_datatype column contains XSD type URIs, which means callers must manually cast. You could add a typed_objects = true option that returns object as UNION(str VARCHAR, int BIGINT, dbl DOUBLE, bool BOOLEAN, ...) or at minimum apply automatic DuckDB type coercion based on the datatype column.

2. **Source filename column** ✅
When reading multiple files via glob, there's no way to know which triple came from which file. Adding a filename column (like DuckDB's read_parquet does) would be very useful for tracing provenance.

3. **read_rdf_prefixes() table function** ✅
A companion function that returns the prefix declarations (@prefix / @base) from a Turtle/TriG file. Useful for documentation and for building CURIE-aware tooling.
Implemented in `src/read_rdf_prefixes.cpp`. Returns three columns: `prefix` (VARCHAR), `uri` (VARCHAR), `is_base` (BOOLEAN). Supports the same `strict_parsing`, `file_type`, and `filename` parameters as `read_rdf()`, and glob patterns. Throws `InvalidInputException` for NTriples, NQuads, and RDF/XML.

4. **SPARQL endpoint reader** ✅
`read_sparql(endpoint, query)` is now implemented. It sends a SPARQL SELECT against an HTTP/HTTPS endpoint and returns the result set as a table.

### Performance Enhancements
5. **Projection pushdown**  ✅
The scan always writes all 6 columns regardless of what the query selects. DuckDB table functions support ProjectionPushdown — implementing RDFReaderFunc to check input.column_ids and skip populating unused columns would reduce allocations for common queries like SELECT subject, predicate FROM read_rdf(...).

6. **Streaming output for full R2RML mode** ✅
The README itself notes this: ClientContextSQLConnection::execute() materializes the entire result set into a vector<MapSQLRow> before any RDF is written. For large tables this is a significant memory spike. A streaming cursor approach — fetching one chunk at a time and flushing to the Serd writer — would fix this.

7. **Parallel write / sharded output**
R2RMLCopyExecutionMode returns REGULAR_COPY_TO_FILE (single-threaded). For inside-out mode, writing to multiple output shards in parallel (like DuckDB's Parquet writer does) could significantly improve throughput on large datasets.

### Correctness / UX
8. **Better error location reporting** ✅
ErrorCallBack captures the line number but not the column or file path. For glob reads with multiple files, errors are unattributed. Including the file path in the SyntaxException message would save a lot of debugging time.

9. **Skip-count in non-strict mode** ✅
In non-strict mode, malformed lines are silently discarded. Exposing a warning or a skip_count in the result (e.g., as a DuckDB warning or a separate read_rdf_errors() function) would help users detect corrupt data without halting the query.

10. **Turtle prefix output in write mode**
When writing in turtle format, the Serd writer is initialized with an empty environment (serd_env_new(nullptr)), so output URIs are never compressed with @prefix declarations. Parsing the mapping file's prefixes and registering them with the writer would produce much more readable Turtle output.

11. **Set datatypes (e.g. xsd) on object for R2RML output** ✅
Currently all literals are output as strings. An improvement would be to convert to xsd types.

12. **Refactor file handling** ✅ (partial) Glob expansion and list-of-files parsing now go through DuckDB's `MultiFileReader::CreateFileList` (see `src/rdf_multi_file.cpp`), shared across all four table functions, and `include_filenames` was renamed to `filename` to match `read_csv`/`read_parquet`. Full adoption of the `MultiFileFunction<T>` template (auto-injected virtual filename column, `union_by_name`, `hive_partitioning`) would still require rewriting every reader as a `BaseFileReader` driven by DuckDB's async `Scan()`/`PrepareScan()` row-group scanning model — RDF's per-file triple streaming doesn't map cleanly onto that, so it was deliberately not pursued here. This work is likely required for 13.1 

13. **Numerous read path performance enhancements.** 
1. *Intra-file parallelism* for line-based formats. ✅ (read_rdf only) Large, uncompressed, seekable NTriples/NQuads files are now split into 64MB-target byte ranges (`RDF_TARGET_RANGE_BYTES` in `src/rdf_extension.cpp`), each scanned by an independent `SerdRangeBuffer` (`src/serd_range_buffer.cpp`/`.hpp`) — a `SerdBuffer` subclass that seeks to its range, aligns forward to the next newline, and stops after finishing the statement in flight at its nominal end. `MaxThreads()` is now `whole_files + ranges` summed over files. A `parallel_scan` named parameter (default true) can force the classic whole-file path. Turtle/TriG/RDF-XML, compressed files, and non-seekable handles are unaffected — they still use the original one-thread-per-file path. `profile_rdf`/`pivot_rdf` were intentionally left out of scope: both aggregate per-file in memory (profiling stats, per-subject pivoting) and would need a merge-across-ranges step before emission to parallelize the same way.
2. *Tiny I/O buffer.* ✅ The serd stream page size is hard-coded to 4096 bytes (src/serd_buffer.cpp:101), meaning a FileHandle::Read call every 4 KB — expensive over httpfs especially. Bumping this to 256 KB–1 MB is a one-line change and probably the cheapest win available.
3. *Filter pushdown.* ✅ filter_pushdown is unset, so WHERE predicate = '...' (extremely common in RDF workloads) materializes every triple into the string heap and filters afterward. Evaluating simple equality/prefix filters on subject/predicate/graph inside the statement callback — before AddString — would skip the copy for non-matching rows and shrink rows flowing upstream. Serd still parses everything, but materialization is often the bigger cost.
4. *Repeated-string cost.* RDF is massively repetitive: typically a handful of distinct predicates, and graph is often constant per file, yet every occurrence gets a fresh heap copy. A small per-chunk cache (serd node bytes → already-added string_t, valid within a chunk) for the predicate/graph columns would eliminate most of those copies. Same idea applies to prefix_expansion=true, which calls serd_env_expand_node (an allocation) per node per triple with no memoization (src/serd_buffer.cpp:110-118) — caching CURIE→expanded-IRI would help a lot there. Relatedly, the filename column loop could use a ConstantVector instead of a flat per-row write (src/rdf_extension.cpp:164-177).
5. *Missing planner/UX hooks.* ✅ No cardinality callback — a cheap estimate from total file bytes ÷ average bytes-per-triple (per format) would help join planning. No table_scan_progress — easy to implement from SeekPosition()/GetFileSize(), pure UX but users notice. No get_batch_index (matters if you do #1, and enables order preservation).
6. *MultiFileReader framework.* ✅ Glob expansion and `LIST[VARCHAR]` file-list parsing now go through `MultiFileReader::CreateFileList` (`src/rdf_multi_file.cpp`), shared by all four table functions, and the `include_filenames` parameter was renamed to `filename` to match core DuckDB naming. File lists are still fully expanded at bind time (true lazy expansion, and the automatic virtual filename column, would require the full `MultiFileFunction<T>`/`BaseFileReader` integration described in item 12).

Baseline:
```
D select count(*) from read_rdf('../geoNames/geonames.nt');
┌──────────────────┐
│   count_star()   │
│      int64       │
├──────────────────┤
│    181846462     │
│ (181.85 million) │
└──────────────────┘
Run Time (s): real 85.058 user 79.177983 sys 4.971778
```
Improved buffer:
```
Run Time (s): real 81.362 user 78.447340 sys 2.612139
```
Baseline for filter pushdown
```
memory D select count(*) from read_rdf('../geoNames/geonames.nt') 
         where predicate = 'http://www.geonames.org/ontology#parentADM4'
         ;
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│       495703 │
└──────────────┘
Run Time (s): real 90.805 user 176.509459 sys 4.014763
```
With pushdown
```
Run Time (s): real 82.206 user 79.360362 sys 2.483339
```