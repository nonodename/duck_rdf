cmake_minimum_required(VERSION 3.10...3.29)

# ------------------------------------------------------------
# Project setup
# ------------------------------------------------------------
set(TARGET_NAME rdf)
set(SERD_NAME serd_lib)

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
FetchContent_MakeAvailable(serd)

FetchContent_Declare(
  sql2rdf
  GIT_REPOSITORY https://github.com/nonodename/sql2rdf.git
  GIT_TAG v1.2.2
)

FetchContent_MakeAvailable(sql2rdf)

# sql2rdf_r2rml/sql2rdf_yarrrml/serd are linked into rdf_extension /
# rdf_loadable_extension, which puts them in DuckDBExports (see
# duckdb/CMakeLists.txt's unconditional install(EXPORT)/export(EXPORT) of
# that set). sql2rdf's own CMakeLists declares their PUBLIC include dirs as
# plain paths under the FetchContent build tree, which CMake refuses to
# export as-is ("... is prefixed in the build directory"). Rewrap them in
# $<BUILD_INTERFACE:...> so they're legal to include in an export set; we
# don't ship an INSTALL_INTERFACE since nothing here actually installs
# sql2rdf's headers for downstream consumption.
foreach(_sql2rdf_export_target sql2rdf_r2rml sql2rdf_yarrrml serd)
    get_target_property(_sql2rdf_includes ${_sql2rdf_export_target} INTERFACE_INCLUDE_DIRECTORIES)
    if(_sql2rdf_includes)
        list(TRANSFORM _sql2rdf_includes PREPEND "$<BUILD_INTERFACE:")
        list(TRANSFORM _sql2rdf_includes APPEND ">")
        set_target_properties(${_sql2rdf_export_target} PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${_sql2rdf_includes}")
    endif()
endforeach()
unset(_sql2rdf_includes)
unset(_sql2rdf_export_target)

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
        sql2rdf_yarrrml
)
target_link_libraries(${LOADABLE_EXTENSION_NAME}
        ${SERD_NAME}
        sql2rdf_yarrrml
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
    # By using INTERFACE/PUBLIC, any target linking to this lib
    # automatically gets the STATIC definition.
    target_compile_definitions(${SERD_NAME} PUBLIC SERD_STATIC)
endif()

# sql2rdf_r2rml/sql2rdf_yarrrml (and the serd copy vendored inside sql2rdf,
# which sql2rdf_r2rml links PUBLIC) are linked into the extension targets
# above, which puts them in ${DUCKDB_EXPORT_SET} (see duckdb/CMakeLists.txt's
# unconditional install(EXPORT)/export(EXPORT) of that set). Their include
# dirs were rewrapped in $<BUILD_INTERFACE:...> above so this is legal.
install(
    TARGETS sql2rdf_r2rml sql2rdf_yarrrml serd
    EXPORT "${DUCKDB_EXPORT_SET}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"  # .a file on Unix, .lib on Windows
)

install(
    TARGETS ${SERD_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"  # .a file on Unix, .lib on Windows
)

# yaml-cpp is privately linked by sql2rdf_yarrrml; since that target is part of
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
