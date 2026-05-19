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

# The iceberg_hive library: a Hive Metastore (HMS) catalog client built on
# generated Apache Thrift bindings. Layout mirrors iceberg_rest.

# ----------------------------------------------------------------------
# Hive Metastore Thrift bindings, generated at build time so the
# checked-in IDL is the only source of truth.

set(_hive_idl_dir ${PROJECT_SOURCE_DIR}/third_party/hive_metastore)
set(_hive_idl ${_hive_idl_dir}/hive_metastore.thrift)
set(_fb303_idl ${_hive_idl_dir}/share/fb303/if/fb303.thrift)
set(_thrift_gen_dir ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp)

set(ICEBERG_HIVE_THRIFT_GEN_SOURCES
    ${_thrift_gen_dir}/FacebookService.cpp
    ${_thrift_gen_dir}/fb303_types.cpp
    ${_thrift_gen_dir}/hive_metastore_constants.cpp
    ${_thrift_gen_dir}/hive_metastore_types.cpp
    ${_thrift_gen_dir}/ThriftHiveMetastore.cpp)

set(ICEBERG_HIVE_THRIFT_GEN_HEADERS
    ${_thrift_gen_dir}/FacebookService.h
    ${_thrift_gen_dir}/fb303_types.h
    ${_thrift_gen_dir}/hive_metastore_constants.h
    ${_thrift_gen_dir}/hive_metastore_types.h
    ${_thrift_gen_dir}/ThriftHiveMetastore.h)

add_custom_command(OUTPUT ${ICEBERG_HIVE_THRIFT_GEN_SOURCES}
                          ${ICEBERG_HIVE_THRIFT_GEN_HEADERS}
                   COMMAND ${CMAKE_COMMAND} -E make_directory ${_thrift_gen_dir}
                   COMMAND thrift::compiler -r --gen cpp:no_skeleton -out
                           ${_thrift_gen_dir} -I ${_hive_idl_dir} ${_hive_idl}
                   DEPENDS thrift::compiler ${_hive_idl} ${_fb303_idl}
                   COMMENT "Generating Hive Metastore C++ bindings from Thrift IDL"
                   VERBATIM)

# Treat generated files as third-party so clang-tidy / clang-format
# don't try to lint them, and so static analysis won't flag style
# issues that originate in the upstream Thrift code generator.
set_source_files_properties(${ICEBERG_HIVE_THRIFT_GEN_SOURCES}
                            ${ICEBERG_HIVE_THRIFT_GEN_HEADERS}
                            PROPERTIES GENERATED TRUE SKIP_LINTING TRUE)

# Standalone target so `ninja iceberg_hive_thrift_bindings` exercises the
# code generator without building the full iceberg_hive library.
add_custom_target(iceberg_hive_thrift_bindings DEPENDS ${ICEBERG_HIVE_THRIFT_GEN_SOURCES}
                                                       ${ICEBERG_HIVE_THRIFT_GEN_HEADERS})

# ----------------------------------------------------------------------
# iceberg_hive library

if(NOT TARGET thrift::thrift)
  message(FATAL_ERROR "iceberg_hive requires a `thrift::thrift` target. With "
                      "ICEBERG_BUILD_BUNDLE=ON this is created by Apache Arrow's bundled "
                      "build; a system-Thrift fallback for the BUILD_BUNDLE=OFF case is "
                      "not yet wired up. Build with -DICEBERG_BUILD_BUNDLE=ON for now.")
endif()

set(ICEBERG_HIVE_SOURCES hive_catalog.cc hive_catalog_properties.cc
                         ${ICEBERG_HIVE_THRIFT_GEN_SOURCES})

set(ICEBERG_HIVE_STATIC_BUILD_INTERFACE_LIBS)
set(ICEBERG_HIVE_SHARED_BUILD_INTERFACE_LIBS)
set(ICEBERG_HIVE_STATIC_INSTALL_INTERFACE_LIBS)
set(ICEBERG_HIVE_SHARED_INSTALL_INTERFACE_LIBS)

list(APPEND ICEBERG_HIVE_STATIC_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_static>,iceberg_static,iceberg_shared>" thrift::thrift)
list(APPEND ICEBERG_HIVE_SHARED_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_shared>,iceberg_shared,iceberg_static>" thrift::thrift)
list(APPEND
     ICEBERG_HIVE_STATIC_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_static>,iceberg::iceberg_static,iceberg::iceberg_shared>"
     thrift::thrift)
list(APPEND
     ICEBERG_HIVE_SHARED_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_shared>,iceberg::iceberg_shared,iceberg::iceberg_static>"
     thrift::thrift)

add_iceberg_lib(iceberg_hive
                SOURCES
                ${ICEBERG_HIVE_SOURCES}
                PRIVATE_INCLUDES
                ${_thrift_gen_dir}
                SHARED_LINK_LIBS
                ${ICEBERG_HIVE_SHARED_BUILD_INTERFACE_LIBS}
                STATIC_LINK_LIBS
                ${ICEBERG_HIVE_STATIC_BUILD_INTERFACE_LIBS}
                STATIC_INSTALL_INTERFACE_LIBS
                ${ICEBERG_HIVE_STATIC_INSTALL_INTERFACE_LIBS}
                SHARED_INSTALL_INTERFACE_LIBS
                ${ICEBERG_HIVE_SHARED_INSTALL_INTERFACE_LIBS})

iceberg_install_all_headers(iceberg/catalog/hive)
