# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(rdf
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    LINKED_LIBS "$<TARGET_FILE:serd_lib>;$<TARGET_FILE:sql2rdf>;$<TARGET_FILE:LibXml2::LibXml2>;$<TARGET_FILE:CURL::libcurl>"
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)