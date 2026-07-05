# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
# WASM side modules are linked by a separate `emcc -sSIDE_MODULE=2 ... ${TO_BE_LINKED}` step
# (see duckdb/extension/extension_build_tools.cmake) that ignores target_link_libraries().
# Only the archives named in LINKED_LIBS are passed to that emcc invocation.
# CURL has no WASM-compatible build, so it is excluded for Emscripten targets; LibXml2
# is also excluded to keep the WASM module minimal (RDF/XML parsing is disabled in WASM).
if (EMSCRIPTEN)
    duckdb_extension_load(rdf
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        LOAD_TESTS
        LINKED_LIBS "$<TARGET_FILE:serd_lib>;$<TARGET_FILE:sql2rdf_lib>;$<TARGET_FILE:yaml-cpp>"
    )
else()
    duckdb_extension_load(rdf
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        LOAD_TESTS
        LINKED_LIBS "$<TARGET_FILE:serd_lib>;$<TARGET_FILE:sql2rdf_lib>;$<TARGET_FILE:LibXml2::LibXml2>;$<TARGET_FILE:CURL::libcurl>"
    )
endif()

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
