# Function Reference

## `read_rdf(path, [options])`

Table function. Reads one or more RDF files and returns their triples as rows.

**Parameters**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `path` | VARCHAR | Yes | — | File path or glob pattern |
| `strict_parsing` | BOOLEAN | No | `true` | When `false`, permits malformed URIs instead of raising an error |
| `prefix_expansion` | BOOLEAN | No | `false` | Expand CURIE-form URIs to full URIs. Ignored for NTriples and NQuads |
| `file_type` | VARCHAR | No | auto-detect | Override format detection. Values: `ttl`, `turtle`, `nq`, `nquads`, `nt`, `ntriples`, `trig`, `rdf`, `xml` |
| `include_filenames` | BOOLEAN | No | `false` | When `true`, adds a 7th column `filename` containing the source file path for each triple |

**Returns**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `graph` | VARCHAR | Yes | Named graph URI; `NULL` for triple-only formats |
| `subject` | VARCHAR | No | Subject URI or blank node |
| `predicate` | VARCHAR | No | Predicate URI |
| `object` | VARCHAR | No | Object value (URI, blank node, or literal) |
| `object_datatype` | VARCHAR | Yes | XSD datatype URI for typed literals; otherwise `NULL` |
| `object_lang` | VARCHAR | Yes | BCP 47 language tag for language-tagged literals; otherwise `NULL` |
| `filename` | VARCHAR | Yes | Source file path; only present when `include_filenames = true` |

**Supported formats**

| Format | Extensions |
|--------|-----------|
| Turtle | `.ttl` |
| NTriples | `.nt` |
| NQuads | `.nq` |
| TriG | `.trig` |
| RDF/XML | `.rdf`, `.xml` |

**Examples**

```sql
-- Read a single file
SELECT subject, predicate, object FROM read_rdf('data.ttl');

-- Read multiple files with a glob pattern
SELECT COUNT(*) FROM read_rdf('shards/*.nt');

-- Override format detection, disable strict parsing
SELECT * FROM read_rdf('data/*.dat', file_type = 'ttl', strict_parsing = false);

-- Expand CURIE-form URIs in a Turtle file
SELECT * FROM read_rdf('data.ttl', prefix_expansion = true);

-- Show which file each triple came from (useful with glob patterns)
SELECT subject, filename FROM read_rdf('shards/*.nt', include_filenames = true);

-- Count triples per source file
SELECT filename, COUNT(*) AS triple_count
FROM read_rdf('data/*.nt', include_filenames = true)
GROUP BY filename
ORDER BY filename;
```

---

## `read_rdf_prefixes(path, [options])`

Table function. Reads one or more Turtle or TriG files and returns their `@prefix` and `@base` declarations as rows. Useful for namespace introspection, documentation, and building CURIE-aware tooling.

Throws an error for NTriples, NQuads, and RDF/XML, as those formats do not contain prefix declarations.

**Parameters**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `path` | VARCHAR | Yes | — | File path or glob pattern |
| `strict_parsing` | BOOLEAN | No | `true` | When `false`, skips malformed content instead of raising an error |
| `file_type` | VARCHAR | No | auto-detect | Override format detection. Values: `ttl`, `turtle`, `trig` |
| `include_filenames` | BOOLEAN | No | `false` | When `true`, adds a 4th column `filename` containing the source file path |

**Returns**

| Column | Type | Description |
|--------|------|-------------|
| `prefix` | VARCHAR | Prefix name; `NULL` for `@base` declarations (which have no prefix name) |
| `uri` | VARCHAR | Namespace URI |
| `is_base` | BOOLEAN | `true` for `@base` declarations, `false` for `@prefix` declarations |
| `filename` | VARCHAR | Source file path; only present when `include_filenames = true` |

**Supported formats**

| Format | Extensions |
|--------|-----------|
| Turtle | `.ttl` |
| TriG | `.trig` |

**Examples**

```sql
-- List all prefixes declared in a Turtle file
SELECT prefix, uri FROM read_rdf_prefixes('data.ttl');

-- Find the base URI
SELECT uri FROM read_rdf_prefixes('data.ttl') WHERE is_base = true;

-- Collect all prefixes from multiple files
SELECT DISTINCT prefix, uri
FROM read_rdf_prefixes('ontologies/*.ttl')
ORDER BY prefix;

-- Show which file each prefix came from
SELECT filename, prefix, uri
FROM read_rdf_prefixes('ontologies/*.ttl', include_filenames = true)
ORDER BY filename, prefix;

-- Count prefix declarations per file across a glob
SELECT filename, COUNT(*) AS prefix_count
FROM read_rdf_prefixes('data/*.ttl', include_filenames = true)
GROUP BY filename
ORDER BY prefix_count DESC;
```

---

## `profile_rdf(path, [options])`

Table function. Reads one or more RDF files and returns a statistical profile with one row per unique predicate. Useful for exploring an unfamiliar dataset, understanding its type distribution, and validating data quality before building a full pipeline.

**Parameters**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `path` | VARCHAR | Yes | — | File path or glob pattern |
| `strict_parsing` | BOOLEAN | No | `true` | When `false`, skips malformed triples instead of raising an error |
| `file_type` | VARCHAR | No | auto-detect | Override format detection. Same values as `read_rdf` |

**Returns**

| Column | Type | Description |
|--------|------|-------------|
| `predicate` | VARCHAR | Predicate URI |
| `types` | VARCHAR[] | Sorted list of distinct DuckDB type names observed for the objects of this predicate |
| `count` | MAP(VARCHAR, UBIGINT) | Number of objects seen per type |
| `min` | MAP(VARCHAR, VARCHAR) | Lexicographic minimum object value per type |
| `max` | MAP(VARCHAR, VARCHAR) | Lexicographic maximum object value per type |
| `graph_count` | UBIGINT | Number of distinct graphs (named or default) this predicate appears in |
| `subject_count` | UBIGINT | Number of distinct subjects this predicate appears with |

**Type names**

Object type names follow DuckDB conventions and are derived from XSD datatypes (see `docs/typemapping.md`). Two additional labels are used for non-literal objects:

| Label | Meaning |
|-------|---------|
| `IRI` | Object is an IRI (resource reference) |
| `BLANK` | Object is a blank node |
| `VARCHAR` | Plain or language-tagged string literal, or `xsd:string` |

Unknown XSD datatypes are reported as their full URI.

**Examples**

```sql
-- Quick overview of all predicates in a file
SELECT predicate, types, count FROM profile_rdf('data.ttl');

-- Find predicates that carry integer values
SELECT predicate, count['HUGEINT'] AS n
FROM profile_rdf('data.nt')
WHERE list_contains(types, 'HUGEINT')
ORDER BY n DESC;

-- Inspect value ranges for a specific predicate
SELECT min['TIMESTAMP'], max['TIMESTAMP']
FROM profile_rdf('events.nt')
WHERE predicate = 'http://schema.org/startDate';

-- Profile all files in a directory
SELECT predicate, subject_count, graph_count
FROM profile_rdf('shards/*.nt')
ORDER BY subject_count DESC;
```

---
## `pivot_rdf(path, [options])`

Table function. Reads one or more RDF files and returns a wide table with at least one row per subject and one predicate per column. Requires two passes of the RDF passed, first essentially uses profile_rdf to compute the schema for the table. Types of the predicate columns will be set based on what is encountered. 

In principle this will work for arbitrary size RDF files (unlike doing a pivot in the SQL domain)

**Parameters**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `path` | VARCHAR | Yes | — | File path or glob pattern |
| `strict_parsing` | BOOLEAN | No | `true` | When `false`, skips malformed triples instead of raising an error |
| `file_type` | VARCHAR | No | auto-detect | Override format detection. Same values as `read_rdf` |

**Returns**

| Column | Type | Description |
|--------|------|-------------|
| `graph` | VARCHAR | Named graph, if available |
| `subject` | VARCHAR | Subject encountered in the RDF|
| _varies_ | _varies_ | One colimn for each unique predicate |

**Column Type Rules**
| Condition |	Column Type |
|-----------|-------------|
| All-lang-tagged predicate |`MAP(VARCHAR, VARCHAR)` — key=language, value=string |
| Single uniform type |`(IRI/BLANK → VARCHAR)`	That DuckDB type (e.g. `HUGEINT`, `VARCHAR)` |
| Mixed types |`UNION(tag := type, ...)` |
| Any above + multi-valued per subject | `LIST(<element_type>)` |

**Examples**

```sql
-- Pivot everything in a trig file
SELECT * FROM pivot_rdf('test/rdf/tests.trig', prefix_expansion=true);
```

---

## `read_sparql(endpoint, query)`

Table function. Sends a SPARQL SELECT query to an HTTP/HTTPS endpoint and returns the result set as a table. Column names match the SPARQL variable names; all columns are VARCHAR.

**Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `endpoint` | VARCHAR | Yes | URL of the SPARQL endpoint (HTTP or HTTPS) |
| `query` | VARCHAR | Yes | SPARQL SELECT query string |

**Returns**

One VARCHAR column per variable named in the SELECT clause. Unbound variables are returned as empty strings.

**Limitations**

- Anonymous (unauthenticated) endpoints only.
- The entire result set is fetched at query-planning time; very large result sets will consume significant memory.

**Examples**

```sql
-- Constant-value query — always returns one row
SELECT x FROM read_sparql(
  'https://query.wikidata.org/sparql',
  'SELECT ?x WHERE { VALUES ?x { "hello" } }'
);

-- Multi-column result from Wikidata
SELECT item, itemLabel FROM read_sparql(
  'https://query.wikidata.org/sparql',
  'SELECT ?item ?itemLabel WHERE {
     ?item wdt:P31 wd:Q146 .
     SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
   } LIMIT 5'
);

-- Aggregate over SPARQL results in DuckDB
SELECT COUNT(*) FROM read_sparql(
  'https://query.wikidata.org/sparql',
  'SELECT ?item WHERE { ?item wdt:P31 wd:Q5 } LIMIT 100'
);
```

---

## `is_valid_r2rml(path)`

Scalar function. Validates an R2RML mapping file.

**Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | VARCHAR | Path to the R2RML mapping file |

**Returns** BOOLEAN — `true` if the file is a valid R2RML mapping, `false` otherwise.

**Example**

```sql
SELECT is_valid_r2rml('mapping.ttl');
```

---

## `can_call_inside_out(path)`

Scalar function. Determines whether an R2RML mapping is usable in inside-out mode (i.e. has no `rr:logicalTable` declarations). Use this to decide which write mode to use.

**Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | VARCHAR | Path to the R2RML mapping file |

**Returns** BOOLEAN — `true` if the mapping is valid for inside-out mode.

**Example**

```sql
SELECT can_call_inside_out('mapping.ttl');
```

---

## `COPY ... TO ... (FORMAT r2rml, ...)`

Copy function. Writes RDF from a DuckDB query using an R2RML mapping.

**Options**

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `mapping` | Yes | — | Path to the R2RML mapping file (`.ttl`) |
| `rdf_format` | No | `ntriples` | Output serialization: `ntriples`, `turtle`, or `nquads` |
| `ignore_non_fatal_errors` | No | `true` | When `true`, logical errors are collected silently. When `false`, the first error raises an exception |
| `ignore_case` | No | `false` | When `true`, all column and table names are lowercased before matching. Use when your R2RML mapping uses lowercase names, which is consistent with DuckDB's lowercase identifier folding. |

**Modes**

**Inside-out mode** — use when `can_call_inside_out()` returns `true`. DuckDB drives the query and passes rows to the extension for mapping:

```sql
COPY (SELECT empno, ename, deptno FROM emp)
TO 'output.nt'
(FORMAT r2rml, mapping 'mapping.ttl');
```

**Full R2RML mode** — use when the mapping contains `rr:logicalTable` declarations. The extension ignores the `COPY` query and runs its own queries from the mapping. Pass a dummy `SELECT 1`:

```sql
COPY (SELECT 1)
TO 'output.nt'
(FORMAT r2rml, mapping 'mapping.ttl');
```

**Example**

```sql
CREATE TABLE emp AS SELECT 7369 AS empno, 'SMITH' AS ename, 10 AS deptno;

COPY (SELECT empno, ename, deptno FROM emp)
TO 'employees.nt'
(FORMAT r2rml, mapping 'mapping.ttl', rdf_format 'turtle');
```
