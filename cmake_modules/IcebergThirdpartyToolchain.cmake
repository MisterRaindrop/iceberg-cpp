# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Accumulate all dependencies to provide suitable static link parameters to the
# third party libraries.
set(ICEBERG_SYSTEM_DEPENDENCIES)
set(ICEBERG_ARROW_INSTALL_INTERFACE_LIBS)

# ----------------------------------------------------------------------
# Versions and URLs for toolchain builds
#
# The following environment variables can be set to customize dependency URLs:
#
# ICEBERG_ARROW_URL          - Apache Arrow tarball URL
# ICEBERG_AVRO_URL           - Apache Avro tarball URL
# ICEBERG_AVRO_GIT_URL       - Apache Avro git repository URL
# ICEBERG_NANOARROW_URL      - Nanoarrow tarball URL
# ICEBERG_CROARING_URL       - CRoaring tarball URL
# ICEBERG_NLOHMANN_JSON_URL  - nlohmann-json tarball URL
# ICEBERG_CPR_URL            - cpr tarball URL
#
# Example usage:
#   export ICEBERG_ARROW_URL="https://your-mirror.com/apache-arrow-24.0.0.tar.gz"
#   cmake -S . -B build
#

set(ICEBERG_ARROW_BUILD_VERSION "24.0.0")
set(ICEBERG_ARROW_BUILD_SHA256_CHECKSUM
    "9a8094d24fa33b90c672ab77fdda253f29300c8b0dd3f0b8e55a29dbd98b82c9")

if(DEFINED ENV{ICEBERG_ARROW_URL})
  set(ARROW_SOURCE_URL "$ENV{ICEBERG_ARROW_URL}")
else()
  set(ARROW_SOURCE_URL
      "https://www.apache.org/dyn/closer.lua?action=download&filename=/arrow/arrow-${ICEBERG_ARROW_BUILD_VERSION}/apache-arrow-${ICEBERG_ARROW_BUILD_VERSION}.tar.gz"
      "https://downloads.apache.org/arrow/arrow-${ICEBERG_ARROW_BUILD_VERSION}/apache-arrow-${ICEBERG_ARROW_BUILD_VERSION}.tar.gz"
      "https://archive.apache.org/dist/arrow/arrow-${ICEBERG_ARROW_BUILD_VERSION}/apache-arrow-${ICEBERG_ARROW_BUILD_VERSION}.tar.gz"
  )
endif()

set(ICEBERG_NANOARROW_BUILD_VERSION "0.8.0")
set(ICEBERG_NANOARROW_BUILD_SHA256_CHECKSUM
    "6e61e2819c9138e9092ba32b568ed6f4594928b306171937251eaaafa7dc2b8c")

if(DEFINED ENV{ICEBERG_NANOARROW_URL})
  set(NANOARROW_SOURCE_URL "$ENV{ICEBERG_NANOARROW_URL}")
else()
  set(NANOARROW_SOURCE_URL
      "https://www.apache.org/dyn/closer.lua?action=download&filename=/arrow/apache-arrow-nanoarrow-${ICEBERG_NANOARROW_BUILD_VERSION}/apache-arrow-nanoarrow-${ICEBERG_NANOARROW_BUILD_VERSION}.tar.gz"
      "https://downloads.apache.org/arrow/apache-arrow-nanoarrow-${ICEBERG_NANOARROW_BUILD_VERSION}/apache-arrow-nanoarrow-${ICEBERG_NANOARROW_BUILD_VERSION}.tar.gz"
      "https://archive.apache.org/dist/arrow/apache-arrow-nanoarrow-${ICEBERG_NANOARROW_BUILD_VERSION}/apache-arrow-nanoarrow-${ICEBERG_NANOARROW_BUILD_VERSION}.tar.gz"
  )
endif()

# Apache Thrift is consumed from the system install (`brew install
# thrift` on macOS, `apt install thrift-compiler libthrift-dev` on
# Debian/Ubuntu). No tarball is fetched here — see
# `resolve_thrift_dependency()` below.

# ----------------------------------------------------------------------
# FetchContent

include(FetchContent)
set(FC_DECLARE_COMMON_OPTIONS)
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
  list(APPEND FC_DECLARE_COMMON_OPTIONS EXCLUDE_FROM_ALL TRUE)
endif()

macro(prepare_fetchcontent)
  set(BUILD_SHARED_LIBS OFF)
  set(BUILD_STATIC_LIBS ON)
  set(CMAKE_COMPILE_WARNING_AS_ERROR FALSE)
  set(CMAKE_EXPORT_NO_PACKAGE_REGISTRY TRUE)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
  # Use "NEW" for CMP0077 by default.
  #
  # https://cmake.org/cmake/help/latest/policy/CMP0077.html
  #
  # option() honors normal variables.
  set(CMAKE_POLICY_DEFAULT_CMP0077
      NEW
      CACHE STRING "")
endmacro()

# ----------------------------------------------------------------------
# Apache Arrow

function(resolve_arrow_dependency)
  prepare_fetchcontent()

  set(ARROW_BUILD_SHARED OFF)
  set(ARROW_BUILD_STATIC ON)
  # Work around undefined symbol: arrow::ipc::ReadSchema(arrow::io::InputStream*, arrow::ipc::DictionaryMemo*)
  set(ARROW_IPC ON)
  set(ARROW_FILESYSTEM ON)
  set(ARROW_S3 ${ICEBERG_S3})
  set(ARROW_JSON ON)
  set(ARROW_PARQUET ON)
  set(ARROW_SIMD_LEVEL "NONE")
  set(ARROW_RUNTIME_SIMD_LEVEL "NONE")
  set(ARROW_POSITION_INDEPENDENT_CODE ON)
  set(ARROW_DEPENDENCY_SOURCE "BUNDLED")
  set(ARROW_WITH_ZLIB ON)
  set(ZLIB_SOURCE "SYSTEM")
  set(ARROW_VERBOSE_THIRDPARTY_BUILD OFF)
  set(CMAKE_CXX_STANDARD 20)

  fetchcontent_declare(VendoredArrow
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL ${ARROW_SOURCE_URL}
                       URL_HASH "SHA256=${ICEBERG_ARROW_BUILD_SHA256_CHECKSUM}"
                       SOURCE_SUBDIR
                       cpp
                       FIND_PACKAGE_ARGS
                       NAMES
                       Arrow
                       CONFIG)

  fetchcontent_makeavailable(VendoredArrow)

  if(vendoredarrow_SOURCE_DIR)
    if(NOT TARGET Arrow::arrow_static)
      add_library(Arrow::arrow_static INTERFACE IMPORTED)
      target_link_libraries(Arrow::arrow_static INTERFACE arrow_static)
      target_include_directories(Arrow::arrow_static
                                 INTERFACE ${vendoredarrow_BINARY_DIR}/src
                                           ${vendoredarrow_SOURCE_DIR}/cpp/src)
    endif()

    if(NOT TARGET Parquet::parquet_static)
      add_library(Parquet::parquet_static INTERFACE IMPORTED)
      target_link_libraries(Parquet::parquet_static INTERFACE parquet_static)
      target_include_directories(Parquet::parquet_static
                                 INTERFACE ${vendoredarrow_BINARY_DIR}/src
                                           ${vendoredarrow_SOURCE_DIR}/cpp/src)
    endif()

    set(ARROW_VENDORED TRUE)
    set_target_properties(arrow_static PROPERTIES OUTPUT_NAME "iceberg_vendored_arrow")
    set_target_properties(parquet_static PROPERTIES OUTPUT_NAME
                                                    "iceberg_vendored_parquet")
    install(TARGETS arrow_static parquet_static
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")

    if(TARGET arrow_bundled_dependencies)
      message(STATUS "arrow_bundled_dependencies found")
      # arrow_bundled_dependencies is only INSTALL_INTERFACE and will not be built by default.
      # We need to add it as a dependency to arrow_static so that it will be built.
      add_dependencies(arrow_static arrow_bundled_dependencies)
      # We cannot install an IMPORTED target, so we need to install the library manually.
      get_target_property(arrow_bundled_dependencies_location arrow_bundled_dependencies
                          IMPORTED_LOCATION)
      install(FILES ${arrow_bundled_dependencies_location}
              DESTINATION ${ICEBERG_INSTALL_LIBDIR})
    endif()

    # Arrow's exported static target interface may reference system libraries
    # (e.g. OpenSSL, CURL, ZLIB) that consumers need to find.
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES ZLIB)
    if(ARROW_S3)
      list(APPEND ICEBERG_SYSTEM_DEPENDENCIES OpenSSL CURL)
    endif()
  else()
    set(ARROW_VENDORED FALSE)
    find_package(Arrow CONFIG REQUIRED)
    find_package(Parquet CONFIG REQUIRED)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES Arrow Parquet)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(ARROW_VENDORED
      ${ARROW_VENDORED}
      PARENT_SCOPE)
endfunction()

# ----------------------------------------------------------------------
# Apache Avro

function(resolve_avro_dependency)
  prepare_fetchcontent()

  set(AVRO_USE_BOOST
      OFF
      CACHE BOOL "" FORCE)

  set(AVRO_BUILD_EXECUTABLES
      OFF
      CACHE BOOL "" FORCE)

  set(AVRO_BUILD_TESTS
      OFF
      CACHE BOOL "" FORCE)

  if(DEFINED ENV{ICEBERG_AVRO_URL})
    # Support custom tarball URL
    fetchcontent_declare(avro-cpp
                         ${FC_DECLARE_COMMON_OPTIONS}
                         URL $ENV{ICEBERG_AVRO_URL}
                             SOURCE_SUBDIR
                             lang/c++
                             FIND_PACKAGE_ARGS
                             NAMES
                             avro-cpp
                             CONFIG)
  else()
    if(DEFINED ENV{ICEBERG_AVRO_GIT_URL})
      set(AVRO_GIT_REPOSITORY "$ENV{ICEBERG_AVRO_GIT_URL}")
    else()
      set(AVRO_GIT_REPOSITORY "https://github.com/apache/avro.git")
    endif()
    fetchcontent_declare(avro-cpp
                         ${FC_DECLARE_COMMON_OPTIONS}
                         GIT_REPOSITORY ${AVRO_GIT_REPOSITORY}
                         GIT_TAG 11fb55500bed9fbe9af53b85112cd13887f0ce80
                         SOURCE_SUBDIR
                         lang/c++
                         FIND_PACKAGE_ARGS
                         NAMES
                         avro-cpp
                         CONFIG)
  endif()

  fetchcontent_makeavailable(avro-cpp)

  # Avro's CMakeLists fetches fmt 10.2.1, which marks
  # `basic_format_string`'s ctor as `consteval` via the FMT_CONSTEVAL
  # macro. With newer Apple clang on macOS 26 (Tahoe) the strict
  # consteval evaluator rejects fmt's own internal FMT_STRING calls
  # (the "compile_string is not a constant expression" error spam in
  # `format-inl.h`). Pre-define `FMT_CONSTEVAL` to empty so fmt's
  # `#ifndef FMT_CONSTEVAL` guard short-circuits to the else branch:
  # `FMT_CONSTEVAL` becomes a no-op (consteval keyword is dropped),
  # `FMT_HAS_CONSTEVAL` is left undefined, and the ctor uses the
  # runtime `check_format_string` path instead. Iceberg core uses
  # `std::format` so we lose nothing.
  #
  # `-DFMT_CONSTEVAL=` (note the trailing `=` to set an empty value)
  # has to go through `target_compile_options` because
  # `target_compile_definitions` strips the trailing `=`.
  foreach(_avro_target IN ITEMS avrocpp avrocpp_s)
    if(TARGET ${_avro_target})
      target_compile_options(${_avro_target} PRIVATE "SHELL:-D FMT_CONSTEVAL=")
    endif()
  endforeach()

  if(avro-cpp_SOURCE_DIR)
    if(NOT TARGET avro-cpp::avrocpp_static)
      add_library(avro-cpp::avrocpp_static INTERFACE IMPORTED)
      target_link_libraries(avro-cpp::avrocpp_static INTERFACE avrocpp_s)
      target_include_directories(avro-cpp::avrocpp_static
                                 INTERFACE ${avro-cpp_BINARY_DIR}
                                           ${avro-cpp_SOURCE_DIR}/lang/c++)
    endif()

    set(AVRO_VENDORED TRUE)
    set_target_properties(avrocpp_s PROPERTIES OUTPUT_NAME "iceberg_vendored_avrocpp")
    set_target_properties(avrocpp_s PROPERTIES POSITION_INDEPENDENT_CODE ON)
    install(TARGETS avrocpp_s
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")

    # TODO: add vendored ZLIB and Snappy support
    find_package(Snappy CONFIG)
    if(Snappy_FOUND)
      list(APPEND ICEBERG_SYSTEM_DEPENDENCIES Snappy)
    endif()
  else()
    set(AVRO_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES Avro)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(AVRO_VENDORED
      ${AVRO_VENDORED}
      PARENT_SCOPE)
endfunction()

# ----------------------------------------------------------------------
# Nanoarrow

# It is also possible to vendor nanoarrow using the bundled source code.
function(resolve_nanoarrow_dependency)
  prepare_fetchcontent()

  fetchcontent_declare(nanoarrow
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL ${NANOARROW_SOURCE_URL}
                       URL_HASH "SHA256=${ICEBERG_NANOARROW_BUILD_SHA256_CHECKSUM}"
                       FIND_PACKAGE_ARGS
                       NAMES
                       nanoarrow
                       CONFIG)
  fetchcontent_makeavailable(nanoarrow)

  if(nanoarrow_SOURCE_DIR)
    if(NOT TARGET nanoarrow::nanoarrow_static)
      add_library(nanoarrow::nanoarrow_static INTERFACE IMPORTED)
      target_link_libraries(nanoarrow::nanoarrow_static INTERFACE nanoarrow_static)
      target_include_directories(nanoarrow::nanoarrow_static
                                 INTERFACE ${nanoarrow_BINARY_DIR}
                                           ${nanoarrow_SOURCE_DIR})
    endif()

    set(NANOARROW_VENDORED TRUE)
    set_target_properties(nanoarrow_static
                          PROPERTIES OUTPUT_NAME "iceberg_vendored_nanoarrow"
                                     POSITION_INDEPENDENT_CODE ON)
    install(TARGETS nanoarrow_static
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
  else()
    set(NANOARROW_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES nanoarrow)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(NANOARROW_VENDORED
      ${NANOARROW_VENDORED}
      PARENT_SCOPE)
endfunction()

# ----------------------------------------------------------------------
# CRoaring

function(resolve_croaring_dependency)
  prepare_fetchcontent()

  set(ENABLE_ROARING_TESTS OFF)
  set(ENABLE_ROARING_MICROBENCHMARKS OFF)

  if(DEFINED ENV{ICEBERG_CROARING_URL})
    set(CROARING_URL "$ENV{ICEBERG_CROARING_URL}")
  else()
    set(CROARING_URL
        "https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v4.4.3.tar.gz")
  endif()

  fetchcontent_declare(croaring
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL ${CROARING_URL}
                           FIND_PACKAGE_ARGS
                           NAMES
                           roaring
                           CONFIG)
  fetchcontent_makeavailable(croaring)

  if(croaring_SOURCE_DIR)
    if(NOT TARGET roaring::roaring)
      add_library(roaring::roaring INTERFACE IMPORTED)
      target_link_libraries(roaring::roaring INTERFACE roaring)
      target_include_directories(roaring::roaring INTERFACE ${croaring_BINARY_DIR}
                                                            ${croaring_SOURCE_DIR}/cpp)
    endif()

    set(CROARING_VENDORED TRUE)
    set_target_properties(roaring PROPERTIES OUTPUT_NAME "iceberg_vendored_croaring"
                                             POSITION_INDEPENDENT_CODE ON)
    install(TARGETS roaring
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
  else()
    set(CROARING_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES roaring)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(CROARING_VENDORED
      ${CROARING_VENDORED}
      PARENT_SCOPE)
endfunction()

# ----------------------------------------------------------------------
# nlohmann-json

function(resolve_nlohmann_json_dependency)
  prepare_fetchcontent()

  set(JSON_BuildTests
      OFF
      CACHE BOOL "" FORCE)

  if(DEFINED ENV{ICEBERG_NLOHMANN_JSON_URL})
    set(NLOHMANN_JSON_URL "$ENV{ICEBERG_NLOHMANN_JSON_URL}")
  else()
    set(NLOHMANN_JSON_URL
        "https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz")
  endif()

  fetchcontent_declare(nlohmann_json
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL ${NLOHMANN_JSON_URL}
                           FIND_PACKAGE_ARGS
                           NAMES
                           nlohmann_json
                           CONFIG)
  fetchcontent_makeavailable(nlohmann_json)

  if(nlohmann_json_SOURCE_DIR)
    if(NOT TARGET nlohmann_json::nlohmann_json)
      add_library(nlohmann_json::nlohmann_json INTERFACE IMPORTED)
      target_link_libraries(nlohmann_json::nlohmann_json INTERFACE nlohmann_json)
      target_include_directories(nlohmann_json::nlohmann_json
                                 INTERFACE ${nlohmann_json_BINARY_DIR}
                                           ${nlohmann_json_SOURCE_DIR})
    endif()

    set(NLOHMANN_JSON_VENDORED TRUE)
    set_target_properties(nlohmann_json
                          PROPERTIES OUTPUT_NAME "iceberg_vendored_nlohmann_json"
                                     POSITION_INDEPENDENT_CODE ON)
    if(MSVC_TOOLCHAIN)
      set(NLOHMANN_NATVIS_FILE ${nlohmann_json_SOURCE_DIR}/nlohmann_json.natvis)
      install(FILES ${NLOHMANN_NATVIS_FILE} DESTINATION .)
    endif()

    install(TARGETS nlohmann_json
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
  else()
    set(NLOHMANN_JSON_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES nlohmann_json)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(NLOHMANN_JSON_VENDORED
      ${NLOHMANN_JSON_VENDORED}
      PARENT_SCOPE)
endfunction()

# ----------------------------------------------------------------------
# zlib

function(resolve_zlib_dependency)
  # use system zlib, zlib is required by arrow and avro
  find_package(ZLIB REQUIRED)
  if(ZLIB_FOUND)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES ZLIB)
    message(STATUS "ZLIB_FOUND ZLIB_LIBRARIES:${ZLIB_LIBRARIES} ZLIB_INCLUDE_DIR:${ZLIB_INCLUDE_DIR}"
    )
    set(ICEBERG_SYSTEM_DEPENDENCIES
        ${ICEBERG_SYSTEM_DEPENDENCIES}
        PARENT_SCOPE)
  endif()

endfunction()

# ----------------------------------------------------------------------
# cpr (C++ Requests)

function(resolve_cpr_dependency)
  prepare_fetchcontent()

  set(CPR_BUILD_TESTS OFF)
  set(CPR_ENABLE_CURL_HTTP_ONLY ON)
  set(CPR_ENABLE_SSL ON)
  set(CPR_USE_SYSTEM_CURL ON)

  if(DEFINED ENV{ICEBERG_CPR_URL})
    set(CPR_URL "$ENV{ICEBERG_CPR_URL}")
  else()
    set(CPR_URL "https://github.com/libcpr/cpr/archive/refs/tags/1.14.1.tar.gz")
  endif()

  fetchcontent_declare(cpr
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL ${CPR_URL}
                           FIND_PACKAGE_ARGS
                           NAMES
                           cpr
                           CONFIG)

  fetchcontent_makeavailable(cpr)

  if(cpr_SOURCE_DIR)
    if(NOT TARGET cpr::cpr)
      add_library(cpr::cpr INTERFACE IMPORTED)
      target_link_libraries(cpr::cpr INTERFACE cpr)
      target_include_directories(cpr::cpr INTERFACE ${cpr_BINARY_DIR}
                                                    ${cpr_SOURCE_DIR}/include)
    endif()

    set(CPR_VENDORED TRUE)
    set_target_properties(cpr PROPERTIES OUTPUT_NAME "iceberg_vendored_cpr"
                                         POSITION_INDEPENDENT_CODE ON)
    add_library(iceberg::cpr ALIAS cpr)
    install(TARGETS cpr
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES OpenSSL CURL)
  else()
    set(CPR_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES cpr)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(CPR_VENDORED
      ${CPR_VENDORED}
      PARENT_SCOPE)
endfunction()

# ----------------------------------------------------------------------
# Zstd

function(resolve_zstd_dependency)
  find_package(zstd CONFIG)
  if(zstd_FOUND)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES zstd)
    message(STATUS "Found zstd, version: ${zstd_VERSION}")
    set(ICEBERG_SYSTEM_DEPENDENCIES
        ${ICEBERG_SYSTEM_DEPENDENCIES}
        PARENT_SCOPE)
  endif()
endfunction()

# ----------------------------------------------------------------------
# Apache Thrift (C++ runtime + IDL compiler, used by iceberg_hive)

function(resolve_thrift_dependency)
  # iceberg_hive needs the Thrift IDL compiler (to regenerate HMS
  # bindings at build time, see catalog/hive/CMakeLists.txt) plus the
  # Thrift C++ runtime to link against. Both are expected to be
  # installed system-wide (e.g. `brew install thrift` on macOS,
  # `apt install thrift-compiler libthrift-dev` on Debian/Ubuntu).
  #
  # We deliberately do NOT vendor Apache Thrift via FetchContent: when
  # ICEBERG_BUILD_BUNDLE=ON, Apache Arrow's bundled-build pipeline
  # creates `thrift::thrift` as an IMPORTED target inside its own
  # build_thrift() helper, which collides with any pre-existing
  # FetchContent target of the same name. Routing both Arrow and
  # iceberg_hive through the system Thrift install avoids that clash
  # and keeps a single Thrift runtime in the final link.
  #
  # This function:
  #   1. On macOS, points Arrow's `FindThriftAlt.cmake` at the brew
  #      install via `Thrift_ROOT`.
  #   2. Locates the thrift IDL compiler and exposes it as
  #      `thrift::compiler` for C04's `add_custom_command` step.
  #
  # The `thrift::thrift` runtime target is created later by Arrow's
  # resolve_dependency(Thrift) (when ICEBERG_BUILD_BUNDLE=ON) or by
  # `iceberg_hive`'s own find_package fallback when the bundle is off.

  if(APPLE
     AND NOT DEFINED Thrift_ROOT
     AND NOT DEFINED ENV{Thrift_ROOT})
    find_program(_brew_executable brew)
    if(_brew_executable)
      execute_process(COMMAND ${_brew_executable} --prefix thrift
                      OUTPUT_VARIABLE _brew_thrift_prefix
                      OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET
                      RESULT_VARIABLE _brew_thrift_status)
      if(_brew_thrift_status EQUAL 0 AND _brew_thrift_prefix)
        set(Thrift_ROOT
            "${_brew_thrift_prefix}"
            CACHE PATH "Apache Thrift install prefix" FORCE)
        message(STATUS "iceberg_hive: using Thrift from ${Thrift_ROOT}")
      endif()
    endif()
  endif()

  find_program(THRIFT_COMPILER_EXECUTABLE
               NAMES thrift
               HINTS "${Thrift_ROOT}/bin")
  if(NOT THRIFT_COMPILER_EXECUTABLE)
    message(FATAL_ERROR "iceberg_hive: ICEBERG_BUILD_HIVE requires the Apache Thrift "
                        "IDL compiler. Install it (e.g. `brew install thrift` on "
                        "macOS or `apt install thrift-compiler` on Debian/Ubuntu) "
                        "and re-run cmake, or set Thrift_ROOT to its install prefix.")
  endif()
  message(STATUS "iceberg_hive: thrift compiler = ${THRIFT_COMPILER_EXECUTABLE}")

  if(NOT TARGET thrift::compiler)
    add_executable(thrift::compiler IMPORTED GLOBAL)
    set_target_properties(thrift::compiler PROPERTIES IMPORTED_LOCATION
                                                      "${THRIFT_COMPILER_EXECUTABLE}")
  endif()

  # `iceberg_hive` exposes `thrift::thrift` in its INSTALL_INTERFACE link
  # libraries, so downstream `find_package(IcebergCpp)` consumers must be
  # able to resolve that target. Record Thrift as a system dependency so
  # `iceberg-config.cmake.in` calls `find_dependency(thrift)` for them.
  # Use the lowercase name to match Apache Thrift's own install path
  # `<prefix>/lib/cmake/thrift/thrift-config.cmake` -- the capital-T form
  # silently misses that directory on case-sensitive Linux filesystems.
  list(APPEND ICEBERG_SYSTEM_DEPENDENCIES thrift)
  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
endfunction()

resolve_zlib_dependency()
resolve_nanoarrow_dependency()
resolve_croaring_dependency()
resolve_nlohmann_json_dependency()

# Resolve Thrift before Arrow so that the iceberg_hive build settings
# (BUILD_COMPILER=ON, BUILD_TESTING=OFF, ...) win over Arrow's bundled
# Thrift declaration. FetchContent_Declare is idempotent on the second
# call with the same name, so Arrow will reuse our vendored Thrift.
if(ICEBERG_BUILD_HIVE)
  resolve_thrift_dependency()
endif()

if(ICEBERG_BUILD_BUNDLE)
  resolve_arrow_dependency()
  resolve_avro_dependency()
  resolve_zstd_dependency()
endif()

if(ICEBERG_BUILD_REST)
  resolve_cpr_dependency()
endif()

# Arrow's build_thrift() creates `thrift::thrift` as an IMPORTED target
# scoped to the FetchContent directory it lives in, so iceberg_hive
# (which is processed in our top-level scope) cannot see it. Promote it
# to a global alias here, after every resolve_*_dependency() has had a
# chance to create the underlying `thrift` target. The `thrift` runtime
# target is non-imported and globally visible by virtue of having been
# created with add_library inside a vendored add_subdirectory.
if(ICEBERG_BUILD_HIVE
   AND TARGET thrift
   AND NOT TARGET thrift::thrift)
  add_library(thrift::thrift INTERFACE IMPORTED GLOBAL)
  target_link_libraries(thrift::thrift INTERFACE thrift)
endif()
