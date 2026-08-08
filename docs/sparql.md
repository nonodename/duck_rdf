# SPARQL 1.1 Support

`rdf` does not embed a SPARQL execution engine. Instead, `sparql_to_sql(sparql, mapping)` and `execute_sparql(sparql, mapping)` **translate** a SPARQL `SELECT`/`ASK` query into an equivalent SQL query, using an R2RML or YARRRML mapping "in reverse" — as a description of which SQL tables and columns each triple pattern corresponds to. `execute_sparql` then runs that SQL spliced directly into the calling query's plan; `sparql_to_sql` just returns the generated SQL text as a string, for inspection or for embedding elsewhere.

This document describes the subset of SPARQL 1.1 that translation supports. For full details, consult the underlying translation library, the badly named [sql2rdf](https://github.com/nonodename/sql2rdf/).

## Rationale

Most SPARQL-over-relational-data tools (e.g. Ontop, D2RQ) run a separate SPARQL engine that issues SQL against the database as a backend. That means a second query language, a second query optimizer, and a second set of type/NULL semantics to reason about — the SPARQL engine's plan and DuckDB's plan are two different worlds glued together at the result-set boundary.

This extension takes a different approach: it compiles SPARQL algebra directly into a single relational-algebra IR and renders **one** DuckDB SQL query, using the R2RML/YARRRML mapping only as a schema — a description of how triples correspond to tables and columns. `execute_sparql`'s `bind_replace` mechanism (the same technique DuckDB's own built-in `query()` table function uses) then hands that generated SQL to DuckDB's binder as if it were a hand-written subquery, so it participates in the *same* plan as everything around it: filters and joins written outside `execute_sparql(...)` get pushed into it, DuckDB's own cardinality estimation and join ordering apply, and there is no second execution engine or result-materialization boundary in between.

Another way to think of it: this extension translates SPARQL to SQL using rules based mapping and optimization. Duck then does cost based optimization on the resulting SQL.

## A Quick Worked Example

Given a table:

```sql
CREATE TABLE emp AS SELECT 7369 AS EMPNO, 'SMITH' AS ENAME, 10 AS DEPTNO;
```

and an R2RML mapping (`emp_map.ttl`):

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.com/ns#> .

<#EmployeeMap>
    rr:logicalTable [ rr:tableName "emp" ] ;
    rr:subjectMap [
        rr:template "http://data.example.com/employee/{empno}" ;
        rr:class ex:Employee ;
    ] ;
    rr:predicateObjectMap [
        rr:predicate ex:name ;
        rr:objectMap [ rr:column "ename" ] ;
    ] .
```

this query:

```sql
SELECT * FROM execute_sparql(
    'PREFIX ex: <http://example.com/ns#>
     SELECT ?e ?name WHERE { ?e ex:name ?name }',
    'emp_map.ttl'
);
```

returns:

| v_e | v_name |
|-----|--------|
| `http://data.example.com/employee/7369` | `SMITH` |

Behind the scenes, `execute_sparql` translated the SPARQL into (and ran, joined into the surrounding plan) this SQL:

```sql
SELECT (t3."v_e") AS "v_e", (t3."v_name") AS "v_name"
FROM (
    SELECT DISTINCT
        ('http://data.example.com/employee/' || CAST(t1."empno" AS VARCHAR)) AS "v_e",
        CAST(t1."ename" AS VARCHAR) AS "v_name"
    FROM "emp" AS t1
    WHERE (t1."empno" IS NOT NULL) AND (t1."ename" IS NOT NULL)
) AS t3
```

`sparql_to_sql(...)` with the same two arguments returns that SQL string directly instead of running it, which is useful for debugging a translation or embedding the generated query somewhere else.

Output columns are always named `v_<sparql-variable-name>`.

## Known Limitations

Translation covers a large, useful subset of SPARQL 1.1 — but it is a *translation*, not an execution engine, and a few constructs are structurally impossible to express as a single relational query over an R2RML mapping. Anything below throws with a specific, named error (surfaced as `SPARQL-to-SQL translation error: ...`) rather than silently producing wrong results.

**Query forms.** Only `SELECT` and `ASK` are supported. `CONSTRUCT` and `DESCRIBE` produce an RDF graph rather than a row set — a fundamentally different translation target — and are rejected.

**Named graphs.** `GRAPH` patterns (and `FROM NAMED`) are rejected. R2RML/YARRRML mappings in this extension never populate `rr:graph`/`rr:graphMap`, so there is no graph dimension for a `GRAPH` pattern to match against.

**Federated query.** `SERVICE` is rejected outright; there is no notion of a second, remote endpoint here.

**Deferred builtin functions.** `ENCODE_FOR_URI()`; the date/time accessors `YEAR()`/`MONTH()`/`DAY()`/`HOURS()`/`MINUTES()`/`SECONDS()`/`TIMEZONE()`/`TZ()`; the non-deterministic functions `NOW()`/`RAND()`/`UUID()`/`STRUUID()`; `SHA384()`/`SHA512()` (DuckDB has no built-in scalar function for either); and any call to a custom, non-builtin (IRI-named) function all throw.

**Out-of-scope variables.** A variable referenced in `FILTER`/`BIND`/`ORDER BY`/`HAVING` that isn't otherwise bound in scope throws at translation time, rather than being treated as unbound per SPARQL's usual semantics.

**Conservative optionality.** A `BIND`'s new variable is always marked "optional" if anything it references is optional, and every variable introduced by a subquery (`{ SELECT ... }`) is always marked "optional" in the enclosing pattern. This never produces wrong results, but can make the generated SQL more defensive (more `LEFT JOIN`/NULL-handling) than strictly necessary.

## Worked Examples

Each example below is self-contained: a SQL schema, a YARRRML mapping, a SPARQL query, and what it returns. (YARRRML is shown because it's more compact than R2RML Turtle for these purposes; anywhere a `.yml`/`.yaml`/`.yarrrml` mapping is accepted, the R2RML Turtle equivalent works identically.)

### 1. Basic triple pattern

Schema:

```sql
CREATE TABLE emp AS SELECT 7369 AS EMPNO, 'SMITH' AS ENAME, 10 AS DEPTNO;
```

Mapping (`emp_map.yml`):

```yaml
prefixes:
  ex: "http://example.com/ns#"

mappings:
  EmployeeMap:
    sources:
      - table: emp
    s: http://data.example.com/employee/$(empno)
    po:
      - [a, ex:Employee]
      - [ex:name, $(ename)]
      - [ex:department, http://data.example.com/department/$(deptno)~iri]
```

Query:

```sql
SELECT * FROM execute_sparql(
    'PREFIX ex: <http://example.com/ns#>
     SELECT ?e ?name WHERE { ?e ex:name ?name }',
    'emp_map.yml'
);
```

Result:

| v_e | v_name |
|-----|--------|
| `http://data.example.com/employee/7369` | `SMITH` |

### 2. Join across two tables (equi-join, `TypeCatalog`-optimized)

This is the first example above extended to a second `mappings:` entry — YARRRML documents can describe any number of tables this way, each becoming an independent `rr:TriplesMap`.

Schema:

```sql
CREATE TABLE emp(empno INTEGER, ename VARCHAR, deptno INTEGER);
INSERT INTO emp VALUES (7369, 'SMITH', 10);

CREATE TABLE dept(deptno INTEGER, dname VARCHAR);
INSERT INTO dept VALUES (10, 'ACCOUNTING');
```

Mapping (`emp_dept_join.yml`):

```yaml
prefixes:
  ex: "http://example.com/ns#"

mappings:
  EmployeeMap:
    sources:
      - table: emp
    s: http://data.example.com/employee/$(empno)
    po:
      - [a, ex:Employee]
      - [ex:name, $(ename)]
      - [ex:empDeptno, $(deptno)]
  DeptMap:
    sources:
      - table: dept
    s: http://data.example.com/department/$(deptno)
    po:
      - [a, ex:Department]
      - [ex:deptDeptno, $(deptno)]
      - [ex:deptName, $(dname)]
```

Query:

```sql
SELECT * FROM execute_sparql(
    'PREFIX ex: <http://example.com/ns#>
     SELECT ?ename ?dname WHERE {
       ?e ex:name ?ename ; ex:empDeptno ?dno .
       ?d ex:deptDeptno ?dno ; ex:deptName ?dname
     }',
    'emp_dept_join.yml'
);
```

Result:

| v_ename | v_dname |
|---------|---------|
| `SMITH` | `ACCOUNTING` |

Because `emp.deptno` and `dept.deptno` are both declared `INTEGER` in DuckDB's catalog, the translator emits a native, uncast join (`t1."deptno" = t2."deptno"`) instead of the always-safe `CAST(... AS VARCHAR)` form it falls back to when column types aren't comparable.

### 3. `OPTIONAL`

Schema:

```sql
CREATE TABLE emp(empno INTEGER, ename VARCHAR, deptno INTEGER);
INSERT INTO emp VALUES (7369, 'SMITH', 10), (7400, 'JONES', NULL);
```

Mapping (`emp_optional.yml`):

```yaml
prefixes:
  ex: "http://example.com/ns#"

mappings:
  EmployeeMap:
    sources:
      - table: emp
    s: http://data.example.com/employee/$(empno)
    po:
      - [a, ex:Employee]
      - [ex:name, $(ename)]
      - [ex:department, http://data.example.com/department/$(deptno)~iri]
```

Query:

```sql
SELECT * FROM execute_sparql(
    'PREFIX ex: <http://example.com/ns#>
     SELECT ?e ?n ?d WHERE {
       ?e ex:name ?n .
       OPTIONAL { ?e ex:department ?d . }
     }',
    'emp_optional.yml'
);
```

Result:

| v_e | v_n | v_d |
|-----|-----|-----|
| `http://data.example.com/employee/7369` | `SMITH` | `http://data.example.com/department/10` |
| `http://data.example.com/employee/7400` | `JONES` | `NULL` |

`ex:department` is left unbound (`NULL`) for JONES because `deptno` is `NULL` for that row and R2RML omits a triple whenever a referenced column is `NULL` — exactly the behavior `OPTIONAL` needs.

### 4. Aggregation (`GROUP BY` / `COUNT`)

Schema and mapping: same `emp_map.yml`/`emp` table as Example 1, but with a second employee row:

```sql
CREATE TABLE emp AS
    SELECT 7369 AS EMPNO, 'SMITH' AS ENAME, 10 AS DEPTNO
    UNION ALL SELECT 7400, 'JONES', 10;
```

Query:

```sql
SELECT * FROM execute_sparql(
    'PREFIX ex: <http://example.com/ns#>
     SELECT ?d (COUNT(?e) AS ?cnt) WHERE {
       ?e ex:department ?d .
     } GROUP BY ?d',
    'emp_map.yml'
);
```

Result:

| v_d | v_cnt |
|-----|-------|
| `http://data.example.com/department/10` | `2` |

### 5. `execute_sparql` as a genuine subquery

Because `execute_sparql` uses DuckDB's `bind_replace` mechanism, it can be joined against, filtered, and column-aliased exactly like a hand-written subquery — the surrounding query and the translated SPARQL are planned as one:

```sql
CREATE TABLE dept AS SELECT 10 AS DEPTNO, 'ACCOUNTING' AS DNAME;

SELECT dept.dname
FROM execute_sparql(
    'PREFIX ex: <http://example.com/ns#>
     SELECT ?e ?name WHERE { ?e ex:name ?name }',
    'emp_map.yml'
) AS t(id, name)
JOIN dept ON dept.deptno = 10;
```

Result: `ACCOUNTING`. An `EXPLAIN` of this query shows no separate `execute_sparql` scan node in the physical plan — the generated SQL was spliced directly into the surrounding query's own plan, so DuckDB's optimizer sees (and can push filters/joins into) the real underlying table scans.
