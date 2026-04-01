This table documents the DuckDB-to-XSD type mappings applied when writing
typed literals via `COPY TO ... (FORMAT r2rml)`.  VARCHAR produces a plain
string literal (no datatype annotation); all other types listed below carry
the corresponding `^^xsd:*` annotation in the output.

| DuckDB Type | XSD Type | Notes |
|---|---|---|
| `BOOLEAN` | `xsd:boolean` | Exact |
| `TINYINT` | `xsd:byte` | signed 8-bit |
| `UTINYINT` | `xsd:unsignedByte` | |
| `SMALLINT` | `xsd:short` | signed 16-bit |
| `USMALLINT` | `xsd:unsignedShort` | |
| `INTEGER` | `xsd:int` | signed 32-bit |
| `UINTEGER` | `xsd:unsignedInt` | |
| `BIGINT` | `xsd:long` | signed 64-bit |
| `UBIGINT` | `xsd:unsignedLong` | |
| `HUGEINT` | `xsd:integer` | arbitrary precision integer; 128-bit so no exact XSD numeric type |
| `UHUGEINT` | `xsd:nonNegativeInteger` | same caveat |
| `FLOAT` | `xsd:float` | IEEE 754 single |
| `DOUBLE` | `xsd:double` | IEEE 754 double |
| `DECIMAL(p,s)` | `xsd:decimal` | Exact |
| `BIGNUM` | `xsd:decimal` | or `xsd:integer` depending on scale |
| `VARCHAR` | `xsd:string` | |
| `DATE` | `xsd:date` | |
| `TIME` | `xsd:time` | |
| `TIMESTAMP` | `xsd:dateTime` | no timezone |
| `TIMESTAMP WITH TIME ZONE` | `xsd:dateTimeStamp` | `xsd:dateTimeStamp` requires timezone (XSD 1.1); fall back to `xsd:dateTime` for XSD 1.0 |
| `INTERVAL` | `xsd:duration` | DuckDB intervals are month/day/microsecond; XSD duration maps cleanly but watch month/day ambiguity |
| `UUID` | — | No XSD type; commonly represented as `xsd:string` or via a custom datatype |
| `BLOB` | `xsd:hexBinary` or `xsd:base64Binary` | Both are valid; `base64Binary` is more common in RDF/OWL |
| `BIT` | `xsd:boolean` | if single bit; `xsd:string` of `"0"`/`"1"` otherwise; no great match |
| `JSON` | — | No XSD type; `xsd:string` in a pinch, or `rdf:JSON` (RDF 1.2 draft) |

A few RDF-specific callouts:
- **`xsd:dateTimeStamp`** is in XSD 1.1 and recognized by OWL 2, but not all triplestores support it — `xsd:dateTime` with an explicit timezone literal is safer.
- **`rdf:JSON`** is being introduced in RDF 1.2 for JSON-valued literals; for now most stores just use `xsd:string`.
- **`xsd:integer`** in XSD is truly unbounded (unlike SQL `INTEGER`), making it the right semantic fit for `HUGEINT` even if DuckDB caps at 128 bits.

