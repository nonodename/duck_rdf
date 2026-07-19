[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![GitHub Release](https://img.shields.io/github/v/release/nonodename/duck_rdf)
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/nonodename/duck_rdf/.github%2Fworkflows%2FMainDistributionPipeline.yml)
[![DuckDB](https://img.shields.io/static/v1?label=duckdb&message=v1.5.4%2B&color=blue)](https://github.com/duckdb/duckdb/releases)
[![Community downloads per week](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fcommunity-extensions.duckdb.org%2Fdownloads-last-week.json&query=%24.rdf&label=downloads%2Fweek&color=brightgreen)](https://duckdb.org/community_extensions/download_metrics)

![Duck RDF Logo](docs/logo.svg)
# A DuckDB extension to work with RDF
Read, write and manipulate RDF within DuckDB. This extension has three broad capabilities
* reading/profiling RDF from standard serializations, [Turtle](http://www.w3.org/TR/turtle/), [NTriples](http://www.w3.org/TR/n-triples/), [NQuads](http://www.w3.org/TR/n-quads/), [TriG](http://www.w3.org/TR/trig/), and [RDF/XML](https://www.w3.org/TR/rdf12-xml/) into a standard columnar schema or pivoted based on observed predicates.
* writing using [R2RML](https://www.w3.org/TR/r2rml/) or [YARRML](https://rml.io/yarrrml/) mappings
* querying either remote sources or using a subset of [SPARQL](https://www.w3.org/TR/sparql11-query/) and the R2RML/Yarrrml mappings in reverse

The extension works across all platforms that DuckDB supports however, note that RDF/XML parsing and `read_sparql()` (which requires OS-level networking via libcurl) are not available in WASM. 

Gzip and Zst compression are supported for reads (for example `.nt.gz`). Zst compression requires the parquet library to be installed and loaded prior to invocation.

## Installation

rdf is a DuckDB Community Extension.

To install and use the extension, run these SQL commands in your DuckDB session:
```
INSTALL rdf FROM community;
LOAD rdf;
```
That's it! The extension is now ready to use.

Full documentation for all the functions can be found in [docs/functions.md](docs/functions.md).

## Building
### Managing dependencies
Where possible VCPKG is used for dependencies. However, libraries like serd are not available on VCPKG, so for these, CMake FetchContent is used instead (e.g. you need an Internet connection at build time.)

Enabling VCPKG is very simple: follow the installation instructions or just run the following:

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
To bring submodules up to same as upstream, run 
```sh
git submodule update --recursive
```
To get the source for DuckDB and CI-tools. Next run: 

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
## Cleanliness

Use `make format` to format all code to the DuckDB standards, `make tidy-check` to lint.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

## Running the tests
Test for this extension are SQL tests in `./test/sql`. They rely on a samples in the test/rdf directory. These SQL tests can be run using:
```sh
make test
```
Note that the SPARQL tests require an internet connection to be able to reach out to Wikidata query service.  

### Installing the deployed binaries directly (e.g. not via community)
To install from GitHub actions:
* navigate to the [actions](https://github.com/nonodename/duck_rdf/actions) for this repo
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

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.