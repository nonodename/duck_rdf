cmake_minimum_required(VERSION 3.10...3.29)

# ------------------------------------------------------------
# Project setup
# ------------------------------------------------------------
set(TARGET_NAME rdf)
set(SERD_NAME serd_lib)
set(SQL2RDF_NAME sql2rdf_lib)

if (NOT EMSCRIPTEN)
    find_package(LibXml2 REQUIRED)
    find_package(CURL REQUIRED)
endif()

project(${TARGET_NAME} LANGUAGES C CXX)
include(FetchContent)

# For yarrml support
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(YAML_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
# Avoid a "format" custom-target name collision with this project's own
# `format` target (below); yaml-cpp's own formatting is irrelevant to us.
set(YAML_CPP_FORMAT_SOURCE OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG yaml-cpp-0.9.0
)

FetchContent_MakeAvailable(yaml-cpp)

FetchContent_Declare(
  serd
  GIT_REPOSITORY https://github.com/drobilla/serd.git
  GIT_TAG v0.32.10
)
FetchContent_GetProperties(serd)
if(NOT serd_POPULATED)
  FetchContent_Populate(serd)
endif()

set(EXTENSION_NAME         ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(EXTENSION_SOURCES
    src/rdf_extension.cpp
    src/r2rml_copy.cpp
    src/serd_buffer.cpp
    src/rdf_profiler.cpp
    src/profile_rdf.cpp
    src/pivot_rdf.cpp
    src/read_rdf_prefixes.cpp
)

# CURL and LibXml2 are not available in Emscripten/WASM builds:
# - CURL requires OS sockets which do not exist in WASM
# - LibXml2 is excluded to keep the WASM module minimal
# These source files are compiled only for native targets.
if (NOT EMSCRIPTEN)
    list(APPEND EXTENSION_SOURCES
        src/rdf_xml_parser.cpp
        src/xml_buffer.cpp
        src/sparql_reader.cpp
    )
endif()

# ------------------------------------------------------------
# SERD — internal static library
# ------------------------------------------------------------
add_library(${SERD_NAME} STATIC
    ${serd_SOURCE_DIR}/src/base64.c
    ${serd_SOURCE_DIR}/src/byte_source.c
    ${serd_SOURCE_DIR}/src/env.c
    ${serd_SOURCE_DIR}/src/n3.c
    ${serd_SOURCE_DIR}/src/node.c
    ${serd_SOURCE_DIR}/src/read_utf8.c
    ${serd_SOURCE_DIR}/src/reader.c
    ${serd_SOURCE_DIR}/src/string.c
    ${serd_SOURCE_DIR}/src/system.c
    ${serd_SOURCE_DIR}/src/uri.c
    ${serd_SOURCE_DIR}/src/writer.c
)

target_include_directories(${SERD_NAME}
    PRIVATE
        ${serd_SOURCE_DIR}/include
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
  sql2rdf/src/yarrrml/YARRRMLParser.cpp
)
target_include_directories(${SQL2RDF_NAME} PRIVATE
    sql2rdf/include
    ${serd_SOURCE_DIR}/include
)
target_link_libraries(${SQL2RDF_NAME} PRIVATE yaml-cpp::yaml-cpp)
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
        ${serd_SOURCE_DIR}/include
        sql2rdf/include
)
target_include_directories(${LOADABLE_EXTENSION_NAME}
    PRIVATE
        src/include
        ${serd_SOURCE_DIR}/include
        sql2rdf/include
)

if (NOT EMSCRIPTEN)
    target_include_directories(${EXTENSION_NAME} PRIVATE ${LIBXML2_INCLUDE_DIR})
    target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${LIBXML2_INCLUDE_DIR})
endif()

# Link internal libs; LibXml2 and CURL are excluded for WASM (no OS sockets, no WASM build of CURL)
target_link_libraries(${EXTENSION_NAME}
        ${SERD_NAME}
        ${SQL2RDF_NAME}
)
target_link_libraries(${LOADABLE_EXTENSION_NAME}
        ${SERD_NAME}
        ${SQL2RDF_NAME}
)

if (NOT EMSCRIPTEN)
    target_link_libraries(${EXTENSION_NAME} LibXml2::LibXml2 CURL::libcurl)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} LibXml2::LibXml2 CURL::libcurl)
endif()

# Signal to C++ that XML and SPARQL features are disabled in WASM builds
if (EMSCRIPTEN)
    target_compile_definitions(${EXTENSION_NAME} PRIVATE DUCK_RDF_NO_XML DUCK_RDF_NO_SPARQL)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE DUCK_RDF_NO_XML DUCK_RDF_NO_SPARQL)
endif()

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

# yaml-cpp is privately linked by ${SQL2RDF_NAME}; since that target is part of
# DUCKDB_EXPORT_SET, CMake requires yaml-cpp to be exported too.
install(
    TARGETS yaml-cpp
    EXPORT "${DUCKDB_EXPORT_SET}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"  # .a file on Unix, .lib on Windows
)
install(
    TARGETS ${EXTENSION_NAME} 
    EXPORT  "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
