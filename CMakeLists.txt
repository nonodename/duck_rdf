cmake_minimum_required(VERSION 3.10...3.29)

# ------------------------------------------------------------
# Project setup
# ------------------------------------------------------------
set(TARGET_NAME rdf)
set(SERD_NAME serd_lib)
set(SQL2RDF_NAME sql2rdf_lib)

find_package(LibXml2 REQUIRED)
find_package(CURL REQUIRED)

project(${TARGET_NAME} LANGUAGES C CXX)

set(EXTENSION_NAME         ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(EXTENSION_SOURCES
    src/rdf_extension.cpp
    src/r2rml_copy.cpp
    src/serd_buffer.cpp
    src/rdf_xml_parser.cpp
    src/xml_buffer.cpp
    src/sparql_reader.cpp
    src/rdf_profiler.cpp
    src/profile_rdf.cpp
    src/pivot_rdf.cpp
)

# ------------------------------------------------------------
# SERD — internal static library
# ------------------------------------------------------------
add_library(${SERD_NAME} STATIC
    serd/src/base64.c
    serd/src/byte_source.c
    serd/src/env.c
    serd/src/n3.c
    serd/src/node.c
    serd/src/read_utf8.c
    serd/src/reader.c
    serd/src/string.c
    serd/src/system.c
    serd/src/uri.c
    serd/src/writer.c
)

target_include_directories(${SERD_NAME}
    PRIVATE
        serd/include
)
#------------------------------------------------------------
# SQL2RDF — internal static library
#------------------------------------------------------------
add_library(${SQL2RDF_NAME} STATIC
  sql2rdf/src/r2rml/R2RMLMapping.cpp
  sql2rdf/src/r2rml/LogicalTable.cpp
  sql2rdf/src/r2rml/BaseTableOrView.cpp
  sql2rdf/src/r2rml/R2RMLView.cpp
  sql2rdf/src/r2rml/TriplesMap.cpp
  sql2rdf/src/r2rml/PredicateObjectMap.cpp
  sql2rdf/src/r2rml/TermMap.cpp
  sql2rdf/src/r2rml/ConstantTermMap.cpp
  sql2rdf/src/r2rml/ColumnTermMap.cpp
  sql2rdf/src/r2rml/TemplateTermMap.cpp
  sql2rdf/src/r2rml/SubjectMap.cpp
  sql2rdf/src/r2rml/PredicateMap.cpp
  sql2rdf/src/r2rml/ObjectMap.cpp
  sql2rdf/src/r2rml/GraphMap.cpp
  sql2rdf/src/r2rml/JoinCondition.cpp
  sql2rdf/src/r2rml/ReferencingObjectMap.cpp
  sql2rdf/src/r2rml/SQLRow.cpp
  sql2rdf/src/r2rml/MapSQLRow.cpp
  sql2rdf/src/r2rml/SQLValue.cpp
  sql2rdf/src/r2rml/StringSQLValue.cpp
  sql2rdf/src/r2rml/R2RMLParser.cpp
)
target_include_directories(${SQL2RDF_NAME} PRIVATE
    sql2rdf/include
    serd/include
)
# ------------------------------------------------------------
# DuckDB extension: static + loadable
# ------------------------------------------------------------
build_static_extension(
    ${TARGET_NAME}
    ${EXTENSION_SOURCES}
)

build_loadable_extension(
    ${TARGET_NAME}
    ""
    ${EXTENSION_SOURCES}
)

# Add target-local include paths
target_include_directories(${EXTENSION_NAME}
    PRIVATE
        src/include
        serd/include
        sql2rdf/include
    ${LIBXML2_INCLUDE_DIR}
)
target_include_directories(${LOADABLE_EXTENSION_NAME}
    PRIVATE
        src/include
        serd/include
        sql2rdf/include
    ${LIBXML2_INCLUDE_DIR}
)

# Link SERD internally (no leakage, no exports)
target_link_libraries(${EXTENSION_NAME}
        ${SERD_NAME}
        ${SQL2RDF_NAME}
        LibXml2::LibXml2
        CURL::libcurl
)

target_link_libraries(${LOADABLE_EXTENSION_NAME}
        ${SERD_NAME}
        ${SQL2RDF_NAME}
        LibXml2::LibXml2
        CURL::libcurl
)

# Windows: force static behavior (avoid dllimport/dllexport mismatches)
if (WIN32)
    # By using INTERFACE/PUBLIC, any target linking to these libs 
    # automatically gets the STATIC definition.
    target_compile_definitions(${SERD_NAME} PUBLIC SERD_STATIC)
    target_compile_definitions(${SQL2RDF_NAME} PUBLIC SQL2RDF_STATIC)
    
    # This ensures the intermediate library knows Serd is static
    target_compile_definitions(${SQL2RDF_NAME} PRIVATE SERD_STATIC)
endif()

install(
    TARGETS ${SQL2RDF_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"  # .a file on Unix, .lib on Windows
)

install(
    TARGETS ${SERD_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"  # .a file on Unix, .lib on Windows
)
install(
    TARGETS ${EXTENSION_NAME} 
    EXPORT  "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
