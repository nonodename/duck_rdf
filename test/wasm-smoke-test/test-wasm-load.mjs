#!/usr/bin/env node
/**
 * WASM smoke test for the duck_rdf extension (regression test for issue #39).
 *
 * Issue #39: the wasm extension built fine but failed to *instantiate* at LOAD
 * time because Serd/LibXml2/curl symbols were left unresolved (target_link_libraries
 * is ignored for the -sSIDE_MODULE link; the libs must also be named in LINKED_LIBS).
 *
 * A successful compile does NOT prove the fix, because SIDE_MODULE link tolerates
 * undefined symbols. The only valid test is to actually LOAD the module in
 * duckdb-wasm and use it. This script does exactly that:
 *
 *   1. starts a tiny static server that serves the locally-built .wasm extension
 *   2. instantiates @duckdb/duckdb-wasm (Node) with unsigned extensions allowed
 *   3. points custom_extension_repository at the local server
 *   4. INSTALL rdf; LOAD rdf;   <-- this is the step that used to fail
 *   5. registers a sample .ttl file and reads it via read_rdf()
 *   6. asserts the expected triples come back
 *
 * Exit code 0 = fix verified. Non-zero = bug still present (or setup problem).
 *
 * Usage:
 *   node test-wasm-load.mjs [path/to/rdf.duckdb_extension.wasm]
 *
 * If no path is given, it auto-discovers under ../build/wasm_eh (and a few
 * other common locations). You can also set RDF_WASM_EXT to the path.
 */

import { createServer } from 'node:http';
import { readFile, readdir, stat } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve, basename, sep } from 'node:path';
import { createRequire } from 'node:module';
import Worker from 'web-worker';
import * as duckdb from '@duckdb/duckdb-wasm';

const require = createRequire(import.meta.url);
const __dirname = dirname(fileURLToPath(import.meta.url));

const EXT_NAME = 'rdf';
const EXT_FILENAME = `${EXT_NAME}.duckdb_extension.wasm`;

// ---------------------------------------------------------------------------
// 1. Locate the locally-built wasm extension
// ---------------------------------------------------------------------------

async function findWasmExtension() {
  // explicit arg / env first
  const explicit = process.argv[2] || process.env.RDF_WASM_EXT;
  if (explicit) {
    const p = resolve(explicit);
    if (!existsSync(p)) throw new Error(`Extension not found at: ${p}`);
    return p;
  }

  // search common build output roots, recursively, for *.duckdb_extension.wasm
  const roots = [
    join(__dirname, '..', 'build', 'wasm_eh'),
    join(__dirname, '..', 'build', 'wasm_mvp'),
    join(__dirname, '..', 'build', 'wasm_threads'),
    join(__dirname, '..', 'build'),
    join(process.cwd(), 'build'),
  ].filter((d) => existsSync(d));

  const matches = [];
  for (const root of roots) {
    await walk(root, matches);
  }
  // prefer the one literally named rdf.duckdb_extension.wasm
  const exact = matches.find((m) => basename(m) === EXT_FILENAME);
  if (exact) return exact;
  if (matches.length) return matches[0];

  throw new Error(
    `Could not find ${EXT_FILENAME}.\n` +
      `Build it first:  make wasm_eh\n` +
      `Then pass the path explicitly, e.g.:\n` +
      `  node test-wasm-load.mjs build/wasm_eh/extension/rdf/${EXT_FILENAME}`,
  );
}

async function walk(dir, out) {
  let entries;
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch {
    return;
  }
  for (const e of entries) {
    const p = join(dir, e.name);
    if (e.isDirectory()) {
      await walk(p, out);
    } else if (e.isFile() && e.name.endsWith('.duckdb_extension.wasm')) {
      out.push(p);
    }
  }
}

// ---------------------------------------------------------------------------
// 2. Tiny static server: respond to ANY request ending in the extension
//    filename with the built artifact. This sidesteps duckdb-wasm's
//    version/platform path construction (e.g. /<repo>/<version>/wasm_eh/rdf...).
// ---------------------------------------------------------------------------

function startExtensionServer(wasmPath) {
  return new Promise(async (resolveServer) => {
    const bytes = await readFile(wasmPath);
    const server = createServer((req, res) => {
      if (req.url && req.url.endsWith('.duckdb_extension.wasm')) {
        res.writeHead(200, {
          'Content-Type': 'application/wasm',
          'Content-Length': bytes.length,
        });
        res.end(bytes);
      } else {
        res.writeHead(404);
        res.end('not found');
      }
    });
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolveServer({ server, url: `http://127.0.0.1:${port}` });
    });
  });
}

// ---------------------------------------------------------------------------
// 3. Instantiate duckdb-wasm in Node
// ---------------------------------------------------------------------------

async function makeDb() {
  // Locate the package's dist/ folder. require.resolve() of the main entry
  // already lands *inside* dist/, so we find the 'dist' segment in that path
  // rather than blindly appending it (which caused the dist/dist error).
  let DUCKDB_DIST;
  try {
    DUCKDB_DIST = join(
      dirname(require.resolve('@duckdb/duckdb-wasm/package.json')),
      'dist',
    );
  } catch {
    // package.json not exposed via exports — derive dist/ from the main entry
    const entry = require.resolve('@duckdb/duckdb-wasm');
    const idx = entry.lastIndexOf(`${sep}dist${sep}`);
    DUCKDB_DIST =
      idx >= 0 ? entry.slice(0, idx + `${sep}dist`.length) : dirname(entry);
  }
  // Use the EH (exception-handling) bundle to match `make wasm_eh`.
  const bundle = {
    mainModule: join(DUCKDB_DIST, 'duckdb-eh.wasm'),
    mainWorker: join(DUCKDB_DIST, 'duckdb-node-eh.worker.cjs'),
  };
  // NOTE: the duckdb Node worker bundle (duckdb-node-eh.worker.cjs) is a
  // CommonJS module. web-worker's default "classic" loader evaluates it with
  // vm.runInThisContext, where `module` is undefined -> the worker throws on
  // startup and instantiate() hangs forever. Passing { type: 'module' } makes
  // web-worker use Node's import() loader, which loads the .cjs correctly.
  const worker = new Worker(bundle.mainWorker, { type: 'module' });
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule);
  // allow_unsigned_extensions MUST be set at open time for wasm
  await db.open({ allowUnsignedExtensions: true });
  return { db, worker };
}

// ---------------------------------------------------------------------------
// sample Turtle data (also written to disk as sample.ttl for reference)
// ---------------------------------------------------------------------------

const SAMPLE_TTL = `@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix ex:   <http://example.org/> .

ex:alice a foaf:Person ;
    foaf:name "Alice" ;
    foaf:knows ex:bob .

ex:bob a foaf:Person ;
    foaf:name "Bob" .
`;

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

function fail(msg, err) {
  console.error(`\n❌  ${msg}`);
  if (err) console.error(String(err.stack || err));
  process.exitCode = 1;
}

async function main() {
  const wasmPath = await findWasmExtension();
  console.log(`Using extension: ${wasmPath}`);

  const { server, url } = await startExtensionServer(wasmPath);
  const { db, worker } = await makeDb();
  const conn = await db.connect();

  try {
    // --- the regression check: does the side-module instantiate & load? ---
    await conn.query(`SET custom_extension_repository = '${url}';`);
    try {
      await conn.query(`INSTALL ${EXT_NAME};`);
      await conn.query(`LOAD ${EXT_NAME};`);
    } catch (e) {
      fail(
        `LOAD ${EXT_NAME} failed — this is the issue #39 symptom ` +
          `(unresolved Serd/LibXml2/curl symbols in the SIDE_MODULE). ` +
          `The fix is NOT working.`,
        e,
      );
      return;
    }
    console.log('✅  LOAD rdf succeeded (module instantiated).');

    // --- exercise the local-file read path (goes through DuckDB FileSystem) ---
    await db.registerFileText('sample.ttl', SAMPLE_TTL);

    const res = await conn.query(
      `SELECT count(*) AS n FROM read_rdf('sample.ttl');`,
    );
    const n = Number(res.toArray()[0].n);
    console.log(`   read_rdf('sample.ttl') returned ${n} triples.`);

    // sample.ttl has 6 triples (2 rdf:type, 2 foaf:name, 1 foaf:knows = 5...)
    // alice: type, name, knows  (3)  +  bob: type, name (2)  = 5
    const EXPECTED = 5;
    if (n !== EXPECTED) {
      fail(
        `Expected ${EXPECTED} triples but got ${n}. ` +
          `LOAD worked, but read_rdf returned unexpected data.`,
      );
      return;
    }

    // also confirm we can pull real values out (proves Serd actually parsed)
    const names = await conn.query(
      `SELECT object FROM read_rdf('sample.ttl')
       WHERE predicate = 'http://xmlns.com/foaf/0.1/name'
       ORDER BY object;`,
    );
    const got = names.toArray().map((r) => r.object);
    console.log(`   foaf:name values: ${JSON.stringify(got)}`);
    if (got.join(',') !== 'Alice,Bob') {
      fail(`Expected names [Alice, Bob] but got ${JSON.stringify(got)}.`);
      return;
    }

    console.log('\n✅  PASS — wasm RDF extension loads and reads local files. Issue #39 is fixed.');
  } finally {
    await conn.close();
    await db.terminate();
    worker.terminate();
    server.close();
  }
}

main().catch((e) => fail('Unexpected error', e));
