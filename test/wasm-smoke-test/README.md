# duck_rdf wasm smoke test (issue #39)

Verifies that the wasm build of the `rdf` extension actually **instantiates and
loads** in DuckDB-Wasm — the precise thing that was broken in
[issue #39](https://github.com/nonodename/duck_rdf/issues/39).

## Why a compile/CI pass isn't enough

The wasm extension is linked with `-sSIDE_MODULE=2`, which **tolerates undefined
symbols at link time**. So the build (and the standard `extension-ci-tools`
distribution pipeline) can go green while the module still fails to instantiate
in the browser/engine because Serd/LibXml2/curl symbols are unresolved. The only
valid test is to actually `LOAD` the module — which is what this script does.

## Steps

From the repo root, build the wasm target on `main`:

```sh
make wasm_eh
```

This produces `build/wasm_eh/.../rdf.duckdb_extension.wasm`.

Then run the test (drop these files into the repo, e.g. as `wasm-smoke-test/`):

```sh
cd wasm-smoke-test
npm install
npm test
```

The script will:

1. auto-discover the built `rdf.duckdb_extension.wasm` under `../build/`
   (or pass a path: `node test-wasm-load.mjs ../build/wasm_eh/extension/rdf/rdf.duckdb_extension.wasm`),
2. serve it from a tiny local HTTP server,
3. instantiate duckdb-wasm with `allow_unsigned_extensions`,
4. `INSTALL rdf; LOAD rdf;` — **the step that used to fail**,
5. register `sample.ttl` and read it with `read_rdf()`,
6. assert it returns the 5 expected triples (and the `foaf:name` values).

Exit code `0` = fix verified. Non-zero (with the issue #39 error message) =
bug still present.

## Notes

- Tests the **local-file** read path only. Per the issue, network fetch via
  `libcurl` (e.g. `read_sparql`, remote URLs) still won't work under wasm — don't
  use those as a test or you'll get a false negative.
- Uses the `wasm_eh` bundle to match `make wasm_eh`. If you build `wasm_mvp`
  instead, the script still finds the artifact, but for an exact engine match you
  can swap the bundle in `makeDb()` to the `mvp` dist files.
- Good candidate to wire into CI as a real in-engine load test (run the build,
  then `npm test`), since the distribution pipeline alone won't catch this class
  of bug.
