# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
# WASM side modules are linked by a separate `emcc -sSIDE_MODULE=2 ... ${TO_BE_LINKED}` step
# (see duckdb/extension/extension_build_tools.cmake) that ignores target_link_libraries().
# Only the archives named in LINKED_LIBS are passed to that emcc invocation, so every static
# lib the extension needs symbols from (serd, sql2rdf's four libraries, yaml-cpp) must be
# named here explicitly and by its real CMake target name (sql2rdf's own CMakeLists.txt
# defines `sql2rdf_r2rml`, `sql2rdf_yarrrml`, `sql2rdf_sparql`, and `sql2rdf_sparql2sql` -
# not a single combined `sql2rdf_lib` target). sql2rdf_sparql/sql2rdf_sparql2sql back the
# sparql_to_sql() scalar function; omitting them here reproduces the exact bug already fixed
# once for sql2rdf_r2rml/sql2rdf_yarrrml (see commits 877095c/ae843df/324769b): the WASM build
# and link stay green, but the module fails to resolve symbols at load time instead.
# LINKED_LIBS must be a single SPACE-separated string, not semicolon-separated: duckdb's
# `duckdb_extension_load` declares LINKED_LIBS as a `oneValueArgs` entry for
# cmake_parse_arguments(), which is called with `${ARGN}` unquoted - a semicolon-joined
# value gets re-split into multiple arguments at that point and cmake_parse_arguments
# silently keeps only the first token, dropping the rest. `extension_build_tools.cmake`
# later runs the stored string through `separate_arguments()` (whitespace-splitting),
# which is what turns a space-separated string back into the real list passed to emcc.
# CURL has no WASM-compatible build, so it is excluded for Emscripten targets; LibXml2
# is also excluded to keep the WASM module minimal (RDF/XML parsing is disabled in WASM).
if (EMSCRIPTEN)
    duckdb_extension_load(rdf
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        LOAD_TESTS
        LINKED_LIBS "$<TARGET_FILE:serd> $<TARGET_FILE:sql2rdf_yarrrml> $<TARGET_FILE:sql2rdf_r2rml> $<TARGET_FILE:sql2rdf_sparql> $<TARGET_FILE:sql2rdf_sparql2sql> $<TARGET_FILE:yaml-cpp>"
    )
else()
    duckdb_extension_load(rdf
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        LOAD_TESTS
        LINKED_LIBS "$<TARGET_FILE:serd> $<TARGET_FILE:sql2rdf_yarrrml> $<TARGET_FILE:sql2rdf_r2rml> $<TARGET_FILE:sql2rdf_sparql> $<TARGET_FILE:sql2rdf_sparql2sql> $<TARGET_FILE:LibXml2::LibXml2> $<TARGET_FILE:CURL::libcurl>"
    )
endif()

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
