# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
# WASM side modules are linked by a separate `emcc -sSIDE_MODULE=2 ... ${TO_BE_LINKED}` step
# (see duckdb/extension/extension_build_tools.cmake) that ignores target_link_libraries().
# Only the archives named in LINKED_LIBS are passed to that emcc invocation, so every static
# lib the extension needs symbols from (serd, sql2rdf's two libraries, yaml-cpp) must be
# named here explicitly and by its real CMake target name (sql2rdf's own CMakeLists.txt
# defines `sql2rdf_r2rml` and `sql2rdf_yarrrml`, not a single combined `sql2rdf_lib` target).
# CURL has no WASM-compatible build, so it is excluded for Emscripten targets; LibXml2
# is also excluded to keep the WASM module minimal (RDF/XML parsing is disabled in WASM).
if (EMSCRIPTEN)
    duckdb_extension_load(rdf
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        LOAD_TESTS
        LINKED_LIBS "$<TARGET_FILE:serd>;$<TARGET_FILE:sql2rdf_yarrrml>;$<TARGET_FILE:sql2rdf_r2rml>;$<TARGET_FILE:yaml-cpp>"
    )
else()
    duckdb_extension_load(rdf
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        LOAD_TESTS
        LINKED_LIBS "$<TARGET_FILE:serd>;$<TARGET_FILE:sql2rdf_yarrrml>;$<TARGET_FILE:sql2rdf_r2rml>;$<TARGET_FILE:LibXml2::LibXml2>;$<TARGET_FILE:CURL::libcurl>"
    )
endif()

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
