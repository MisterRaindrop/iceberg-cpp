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

# Skeleton for the iceberg_hive library target.
#
# Source layout, dependency wiring and `add_iceberg_lib(iceberg_hive ...)`
# are introduced in follow-up commits (C05 registers the actual library
# target). For now this directory:
#
#   * installs the public export header (iceberg_hive_export.h);
#   * generates the Hive Metastore C++ bindings from the vendored
#     Thrift IDL at build time, so they stay in sync with whatever IDL
#     revision the repository checks in.

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

# A custom target so that follow-up commits (C05's `iceberg_hive` lib)
# can depend on the codegen step by name, and so `ninja
# iceberg_hive_thrift_bindings` exercises just the generator.
add_custom_target(iceberg_hive_thrift_bindings DEPENDS ${ICEBERG_HIVE_THRIFT_GEN_SOURCES}
                                                       ${ICEBERG_HIVE_THRIFT_GEN_HEADERS})

iceberg_install_all_headers(iceberg/catalog/hive)
