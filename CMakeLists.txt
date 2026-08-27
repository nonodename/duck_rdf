cmake_minimum_required(VERSION 3.10...3.29)
include(FetchContent)
# ------------------------------------------------------------
# Project setup
# ------------------------------------------------------------
set(TARGET_NAME rdf)
set(SERD_NAME serd)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
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

file(GLOB EXTENSION_SOURCES "src/*.cpp")

# CURL and LibXml2 are not available in Emscripten/WASM builds:
# - CURL requires OS sockets which do not exist in WASM
# - LibXml2 is excluded to keep the WASM module minimal
# These source files are compiled only for native targets.
if (EMSCRIPTEN)
    list(REMOVE_ITEM EXTENSION_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/src/rdf_xml_parser.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/xml_buffer.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/sparql_reader.cpp"
    )
endif()

# ------------------------------------------------------------
# SERD — internal static library
# ------------------------------------------------------------
file(GLOB SERD_C_SOURCES "${serd_SOURCE_DIR}/src/*.c")
add_library(${SERD_NAME} STATIC ${SERD_C_SOURCES})

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
  GIT_TAG v2.2.0
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
        ${sql2rdf_SOURCE_DIR}/src
)
target_include_directories(${LOADABLE_EXTENSION_NAME}
    PRIVATE
        src/include
        ${serd_SOURCE_DIR}/include
        ${sql2rdf_SOURCE_DIR}/src
)

if (NOT EMSCRIPTEN)
    target_include_directories(${EXTENSION_NAME} PRIVATE ${LIBXML2_INCLUDE_DIR})
    target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${LIBXML2_INCLUDE_DIR})
endif()

# Link internal libs; LibXml2 and CURL are excluded for WASM (no OS sockets, no WASM build of CURL).
# sql2rdf::sparql2sql (SPARQL-to-SQL translation) and sql2rdf::type_catalog_loader
# (sql2rdf::loadTypeCatalog, used by BuildTypeCatalog in sparql_to_sql.cpp) have no
# curl/libxml2/DuckDB dependency of their own, so unlike LibXml2/CURL below they are
# linked unconditionally, including for WASM.
target_link_libraries(${EXTENSION_NAME}
        ${SERD_NAME}
        sql2rdf::yarrrml
        sql2rdf::sparql2sql
        sql2rdf::type_catalog_loader
)
target_link_libraries(${LOADABLE_EXTENSION_NAME}
        ${SERD_NAME}
        sql2rdf::yarrrml
        sql2rdf::sparql2sql
        sql2rdf::type_catalog_loader
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
    TARGETS sql2rdf_r2rml sql2rdf_yarrrml sql2rdf_sparql sql2rdf_sparql2sql sql2rdf_type_catalog_loader yaml-cpp ${SERD_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"  # .a file on Unix, .lib on Windows
)

install(
    TARGETS ${EXTENSION_NAME} 
    EXPORT  "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
