# A DuckDB extension to read and write RDF

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, Rdf, allow you to read & write RDF files directly in to/out of DuckDB. The [SERD](https://drobilla.gitlab.io/serd/doc/singlehtml/) libray is used for this, meaning the extension can parse/write [Turtle](http://www.w3.org/TR/turtle/), [NTriples](http://www.w3.org/TR/n-triples/), [NQuads](http://www.w3.org/TR/n-quads/), and [TriG](http://www.w3.org/TR/trig/). An experimental parser is also provideded to read RDF/XML serialization. This is used when the file extension is `.rdf` or `.xml`. No XML write is supported. No one needs that.

## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the installation instructions or just run the following:

```sh
cd <your-working-dir-not-the-plugin-repo>
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg && git checkout ce613c41372b23b1f51333815feb3edd87ef8a8b
sh ./scripts/bootstrap.sh -disableMetrics
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build steps
To build the extension, first clone this repo. Then in the repo base locally run:

```sh
git submodule update --init --recursive
```
(That command also works for updating to the latest version of the submodules) 
To get the source for DuckDB, Serd and CI-tools. Next run: 

```sh
make
```
If you have ninja avilable you can use that for faster builds:
```sh
GEN=ninja make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/rdf/rdf.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `rdf.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

## Reading RDF

Six columns are returned for RDF. Three are always not null:
* subject
* predicate
* object

The other three columns will be null if no value is provided in the underlying RDF file:
* graph
* language_tag
* datatype

 `read_rdf()` takes a file path or glob pattern and returns a table. When a glob pattern matches multiple files, all matching files are read and their triples are combined:
```
D select subject, predicate from read_rdf('test/rdf/tests.nt');
┌───────────────────────────────────┬─────────────────────────────────────────────────┐
│              subject              │                    predicate                    │
│              varchar              │                     varchar                     │
├───────────────────────────────────┼─────────────────────────────────────────────────┤
│ http://example.org/person/JohnDoe │ http://www.w3.org/1999/02/22-rdf-syntax-ns#type │
│ http://example.org/person/JohnDoe │ http://xmlns.com/foaf/0.1/name                  │
│ http://example.org/person/JohnDoe │ http://xmlns.com/foaf/0.1/age                   │
│ http://example.org/person/JohnDoe │ http://xmlns.com/foaf/0.1/knows                 │
│ jane                              │ http://www.w3.org/1999/02/22-rdf-syntax-ns#type │
│ jane                              │ http://xmlns.com/foaf/0.1/name                  │
│ http://example.org/book/123       │ http://purl.org/dc/elements/1.1/title           │
│ http://example.org/book/123       │ http://purl.org/dc/elements/1.1/creator         │
│ http://unicode.org/duck           │ http://example.org/hasEmoji                     │
└───────────────────────────────────┴─────────────────────────────────────────────────┘
```
### Optional Parameters

#### Strict Parsing

The optional parameter `strict_parsing`, defaults to true and exposes the underlying strict parsing feature of the serd RDF parsing library. When false it permits malformed URIs. To disable strict parsing, pass `strict_parsing = false`.

#### Prefix Expansion

The optional parameter `prefix_expansion` defaults to false and exposes the underlying serd `serd_env_expand_node` function to expand [CURIE](https://en.wikipedia.org/wiki/CURIE) form URIs to fully defined URIs. This is applied to all columns and is ignored when parsing ntriples and nquads.

#### File Type override

The optional parameter `file_type` can be used to override the detected file type of the file. The following values are recognized:
 * Turtle: `ttl`, `turtle`
 * NQuads: `nq`, `nquads`
 * NTriples: `nt`, `ntriples`
 * Trig: `trig`
 * RDF/XML `rdf`, `xml`

When using a glob pattern the `file_type` override is applied uniformly to every matched file.

### Glob / multiple files

The path argument accepts glob patterns, allowing multiple RDF files to be read in a single call. All matched files are scanned in parallel and their triples are combined into one result set:

```sql
-- Read all NTriples files in a directory
SELECT COUNT(*) FROM read_rdf('data/shards/*.nt');

-- Mix with other parameters — file_type applies to every matched file
SELECT * FROM read_rdf('data/shards/*.dat', file_type = 'ttl', strict_parsing = false);
```

If the pattern matches no files an `IO Error` is raised.

## Querying SPARQL Endpoints

The experimental `read_sparql(endpoint, query)` sends a SPARQL SELECT query to a remote endpoint and returns the result set as a DuckDB table. Column names are derived from the SPARQL variable names; all columns are VARCHAR. Unbound variables are returned as empty strings.

Only non authenticated SPARQL end points are supported at this time, but a future version could use  `ATTACH` and secrets to bind more complex end points.

```sql
-- Simple value lookup — returns one row with column "x"
SELECT x FROM read_sparql(
    'https://query.wikidata.org/sparql',
    'SELECT ?x WHERE { VALUES ?x { "hello" } }'
);
```

```
┌─────────┐
│    x    │
│ varchar │
├─────────┤
│ hello   │
└─────────┘
```

```sql
-- Count number of humans in wikidata
SELECT * FROM read_sparql(
             'https://query.wikidata.org/sparql',
             'SELECT (COUNT(*) AS ?count) WHERE {   ?item wdt:P31 wd:Q5 .}'
         );
```

```
┌──────────┐
│  count   │
│ varchar  │
├──────────┤
│ 13074374 │
└──────────┘
``` 

**Notes:**
- Only anonymous (unauthenticated) endpoints are supported.
- The entire result set is fetched at query-planning time; very large result sets will consume significant memory.
- Both HTTP and HTTPS endpoints are supported.

## _Experimental_ RDF write support

The extension can also write RDF from DuckDB data using an [R2RML](https://www.w3.org/TR/r2rml/) mapping file, DuckDB's `COPY TO` syntax and the [SQL2RDF++](https://github.com/nonodename/sql2rdf) library. Two modes are supported, and the correct one is chosen automatically based on the mapping.

This write support is **experimental**! It passes the tests but the author doesn't have any production scaled out workload to try this on. If you use it and find issues, please get in touch and contribute issues using the steps below.

### Inside-out mode

Use this when your R2RML mapping has **no** `rr:logicalTable` declarations (i.e. `can_call_inside_out()` returns `true`). DuckDB drives the SQL query and passes each result row to the extension, which maps them to RDF triples using the mapping:

```sql
COPY (SELECT empno, ename, deptno FROM emp)
TO 'output.nt'
(FORMAT r2rml, mapping 'mapping.ttl');
```

Expect this mode to be as performant as single threaded output can be as it follows the idioms for copy export. 

### Full R2RML mode

Use this when your mapping has `rr:logicalTable` declarations that specify which tables to query. The extension ignores the SQL in the `COPY` statement and runs the mapping's own queries against the live DuckDB instance. Pass a dummy `SELECT 1` to satisfy DuckDB's `COPY` syntax:

```sql
COPY (SELECT 1) TO 'output.nt' (FORMAT r2rml, mapping 'mapping.ttl');
```

To be clear, this is a bit of a hack. But it works, under the covers it's a bit ugly. In particular the result set is cached. If a lot of folks end up using this we'll look at a more streaming output here.

### Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `mapping` | Yes | — | Path to the R2RML mapping file (`.ttl`) |
| `rdf_format` | No | `ntriples` | Output RDF serialization: `ntriples`, `turtle`, or `nquads` |
| `ignore_non_fatal_errors` | No | `true` | When `true`, logical parse errors (e.g. unresolved `rr:parentTriplesMap`, unrecognised logical-table type) are collected silently. When `false`, the first such error raises an exception. |

### Example

```sql
-- Create some data
CREATE TABLE emp AS SELECT 7369 AS empno, 'SMITH' AS ename, 10 AS deptno;

-- Write as NTriples using an inside-out mapping
COPY (SELECT empno, ename, deptno FROM emp)
TO 'employees.nt'
(FORMAT r2rml, mapping 'mapping.ttl');

-- Read it back
SELECT subject, predicate, object FROM read_rdf('employees.nt');
```

```
┌───────────────────────────────────────┬─────────────────────────────────────────────────┬───────────────────────────────────────┐
│ subject                               │ predicate                                       │ object                                │
├───────────────────────────────────────┼─────────────────────────────────────────────────┼───────────────────────────────────────┤
│ http://data.example.com/employee/7369 │ http://example.com/ns#department                │ http://data.example.com/department/10 │
│ http://data.example.com/employee/7369 │ http://example.com/ns#name                      │ SMITH                                 │
│ http://data.example.com/employee/7369 │ http://www.w3.org/1999/02/22-rdf-syntax-ns#type │ http://example.com/ns#Employee        │
└───────────────────────────────────────┴─────────────────────────────────────────────────┴───────────────────────────────────────┘
```

### R2RML validation helpers

Two scalar functions are available to validate R2RML mapping files:

```sql
-- Returns true if the file is a valid R2RML mapping
SELECT is_valid_r2rml('mapping.ttl');

-- Returns true if the mapping is valid for inside-out mode (no rr:logicalTable etc.)
SELECT can_call_inside_out('mapping.ttl');
```

## Running the tests
Test for this extension are SQL tests in `./test/sql`. They rely on a samples in the test/rdf directory. These SQL tests can be run using:
```sh
make test
```
Note that the SPARQL tests require an internet connection to be able to reach out to Wikidata query service.  

### Installing the deployed binaries
To install from GitHub actions:
* navigate to the [actions](https://github.com/nonodename/read_rdf/actions) for this repo
* click on the latest successful build (or build for a release)
* select the architecture you want from the left hand navigation
* open the `Run actions/upload artifact` step
* find the artifact URL for the compiled extension
* download, unzip and then [install](https://duckdb.org/docs/stable/extensions/advanced_installation_methods) to DudkDB

To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL rdf
LOAD rdf
```

If you'd like to see this listed as a community extension, please file an issue (or comment on an existing issue for the same) and if there's sufficient demand I'll try and make it happen.

## Reporting bugs

Please report bugs as issues on this project. Provide a sample RDF file that demonstrates the bug as well as steps to reproduce.

## Future work

Potential future enhancements, courtesy Claude, below. If any of these seem interesting to you please indicate via an issue (or implement as a pull request :-)).

## Functional Enhancements
1. Typed object values
Currently all 6 columns are VARCHAR. The object_datatype column contains XSD type URIs, which means callers must manually cast. You could add a typed_objects = true option that returns object as UNION(str VARCHAR, int BIGINT, dbl DOUBLE, bool BOOLEAN, ...) or at minimum apply automatic DuckDB type coercion based on the datatype column.

2. Source filename column
When reading multiple files via glob, there's no way to know which triple came from which file. Adding a filename column (like DuckDB's read_parquet does) would be very useful for tracing provenance.

3. read_rdf_prefixes() table function
A companion function that returns the prefix declarations (@prefix / @base) from a Turtle/TriG file. Useful for documentation and for building CURIE-aware tooling.

4. SPARQL endpoint reader - DONE
`read_sparql(endpoint, query)` is now implemented. It sends a SPARQL SELECT against an HTTP/HTTPS endpoint and returns the result set as a table.

### Performance Enhancements
5. Projection pushdown -DONE
The scan always writes all 6 columns regardless of what the query selects. DuckDB table functions support ProjectionPushdown — implementing RDFReaderFunc to check input.column_ids and skip populating unused columns would reduce allocations for common queries like SELECT subject, predicate FROM read_rdf(...).

6. Streaming output for full R2RML mode
The README itself notes this: ClientContextSQLConnection::execute() materializes the entire result set into a vector<MapSQLRow> before any RDF is written. For large tables this is a significant memory spike. A streaming cursor approach — fetching one chunk at a time and flushing to the Serd writer — would fix this.

7. Parallel write / sharded output
R2RMLCopyExecutionMode returns REGULAR_COPY_TO_FILE (single-threaded). For inside-out mode, writing to multiple output shards in parallel (like DuckDB's Parquet writer does) could significantly improve throughput on large datasets.

### Correctness / UX
8. Better error location reporting - DONE
ErrorCallBack captures the line number but not the column or file path. For glob reads with multiple files, errors are unattributed. Including the file path in the SyntaxException message would save a lot of debugging time.

9. Skip-count in non-strict mode
In non-strict mode, malformed lines are silently discarded. Exposing a warning or a skip_count in the result (e.g., as a DuckDB warning or a separate read_rdf_errors() function) would help users detect corrupt data without halting the query.

10. Turtle prefix output in write mode
When writing in turtle format, the Serd writer is initialized with an empty environment (serd_env_new(nullptr)), so output URIs are never compressed with @prefix declarations. Parsing the mapping file's prefixes and registering them with the writer would produce much more readable Turtle output.
