PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=rdf
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Force MSVC on the windows_amd64 target.
#
# For windows_amd64 the CI activates vcvars64, so vcpkg builds the C/C++
# dependencies (curl, zlib, libxml2) with MSVC. But DuckDB's CMake configure
# runs with "-G Ninja" and no CC/CXX set, so it auto-selects MinGW gcc from
# PATH for DuckDB + this extension. Mixing MinGW objects with MSVC-built libs
# breaks the link with thousands of unresolved MSVC CRT / stdio symbols.
#
# Pinning CC/CXX to cl makes the whole build MSVC, matching the deps. A
# Makefile assignment overrides the workflow's empty CC=/CXX= environment
# value, and export propagates it to the cmake child process. The mingw
# target (windows_amd64_mingw) is intentionally left untouched.
# ---------------------------------------------------------------------------
ifeq ($(DUCKDB_PLATFORM),windows_amd64)
  export CC := cl
  export CXX := cl
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile