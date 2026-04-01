## Future work

Potential future enhancements, courtesy Claude, below. If any of these seem interesting to you please indicate via an issue (or implement as a pull request :-)).

## Functional Enhancements
1. **Typed object values**
Currently all 6 columns are VARCHAR. The object_datatype column contains XSD type URIs, which means callers must manually cast. You could add a typed_objects = true option that returns object as UNION(str VARCHAR, int BIGINT, dbl DOUBLE, bool BOOLEAN, ...) or at minimum apply automatic DuckDB type coercion based on the datatype column.

2. **Source filename column** ✅
When reading multiple files via glob, there's no way to know which triple came from which file. Adding a filename column (like DuckDB's read_parquet does) would be very useful for tracing provenance.

3. **read_rdf_prefixes() table function**
A companion function that returns the prefix declarations (@prefix / @base) from a Turtle/TriG file. Useful for documentation and for building CURIE-aware tooling.

4. **SPARQL endpoint reader** ✅
`read_sparql(endpoint, query)` is now implemented. It sends a SPARQL SELECT against an HTTP/HTTPS endpoint and returns the result set as a table.

### Performance Enhancements
5. **Projection pushdown**  ✅
The scan always writes all 6 columns regardless of what the query selects. DuckDB table functions support ProjectionPushdown — implementing RDFReaderFunc to check input.column_ids and skip populating unused columns would reduce allocations for common queries like SELECT subject, predicate FROM read_rdf(...).

6. **Streaming output for full R2RML mode**
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