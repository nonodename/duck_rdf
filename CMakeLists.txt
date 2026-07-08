cmake_minimum_required(VERSION 3.10...3.29)
include(FetchContent)
# ------------------------------------------------------------
# Project setup
# ------------------------------------------------------------
set(TARGET_NAME rdf)
set(SERD_NAME serd)
set(EXTENSION_NAME         ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

if (NOT EMSCRIPTEN)
    find_package(LibXml2 REQUIRED)
    find_package(CURL REQUIRED)
endif()

project(${TARGET_NAME} LANGUAGES C CXX)

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
FetchContent_MakeAvailable(serd)


set(EXTENSION_SOURCES
    src/rdf_extension.cpp
    src/r2rml_copy.cpp
    src/serd_buffer.cpp
    src/rdf_profiler.cpp
    src/profile_rdf.cpp
    src/pivot_rdf.cpp
    src/read_rdf_prefixes.cpp
    src/table_filter_eval.cpp
    src/rdf_multi_file.cpp
    src/rdf_triples_factory.cpp
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

# This FetchContent needs to be after serd is defined
# as a target, because sql2rdf CMakeLists will skip creating 
# its own serd target if one already exists. The target name
# MUST be `serd` for that to work
FetchContent_Declare(
  sql2rdf
  GIT_REPOSITORY https://github.com/nonodename/sql2rdf.git
  GIT_TAG v1.2.3
)

FetchContent_MakeAvailable(sql2rdf)
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
)
target_include_directories(${LOADABLE_EXTENSION_NAME}
    PRIVATE
        src/include
        ${serd_SOURCE_DIR}/include
)

if (NOT EMSCRIPTEN)
    target_include_directories(${EXTENSION_NAME} PRIVATE ${LIBXML2_INCLUDE_DIR})
    target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${LIBXML2_INCLUDE_DIR})
endif()

# Link internal libs; LibXml2 and CURL are excluded for WASM (no OS sockets, no WASM build of CURL)
target_link_libraries(${EXTENSION_NAME}
        ${SERD_NAME}
        sql2rdf::yarrrml
)
target_link_libraries(${LOADABLE_EXTENSION_NAME}
        ${SERD_NAME}
        sql2rdf::yarrrml
)

# Signal to C++ that XML and SPARQL features are disabled in WASM builds
if (NOT EMSCRIPTEN)
    target_link_libraries(${EXTENSION_NAME} LibXml2::LibXml2 CURL::libcurl)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} LibXml2::LibXml2 CURL::libcurl)
else()
    target_compile_definitions(${EXTENSION_NAME} PRIVATE DUCK_RDF_NO_XML DUCK_RDF_NO_SPARQL)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE DUCK_RDF_NO_XML DUCK_RDF_NO_SPARQL)
endif()

# Windows: force static behavior (avoid dllimport/dllexport mismatches)
if (WIN32)
    # By using INTERFACE/PUBLIC, any target linking to this lib
    # automatically gets the STATIC definition.
    target_compile_definitions(${SERD_NAME} PUBLIC SERD_STATIC)
endif()

install(
    TARGETS sql2rdf_r2rml sql2rdf_yarrrml yaml-cpp ${SERD_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"  # .a file on Unix, .lib on Windows
)

install(
    TARGETS ${EXTENSION_NAME} 
    EXPORT  "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
