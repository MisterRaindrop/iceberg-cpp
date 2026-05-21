# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

iceberg_hadoop_sources = files(
    'hadoop_catalog.cc',
    'hadoop_catalog_properties.cc',
    'hadoop_file_layout.cc',
    'hadoop_lock_manager.cc',
    'hadoop_log.cc',
    'hadoop_table_operations.cc',
    'hadoop_tables.cc',
)

iceberg_hadoop_build_deps = [iceberg_dep]
iceberg_hadoop_lib = library(
    'iceberg_hadoop',
    sources: iceberg_hadoop_sources,
    dependencies: iceberg_hadoop_build_deps,
    gnu_symbol_visibility: 'hidden',
    cpp_shared_args: ['-DICEBERG_HADOOP_EXPORTING'],
    cpp_static_args: ['-DICEBERG_HADOOP_STATIC'],
)

iceberg_hadoop_compile_args = []
if get_option('default_library') == 'static'
    iceberg_hadoop_compile_args += ['-DICEBERG_HADOOP_STATIC']
endif
iceberg_hadoop_dep = declare_dependency(
    link_with: [iceberg_hadoop_lib],
    dependencies: iceberg_hadoop_build_deps,
    compile_args: iceberg_hadoop_compile_args,
)
meson.override_dependency('iceberg-hadoop', iceberg_hadoop_dep)
pkg.generate(iceberg_hadoop_lib)

install_headers(
    [
        'hadoop_catalog.h',
        'hadoop_catalog_properties.h',
        'hadoop_file_layout.h',
        'hadoop_lock_manager.h',
        'hadoop_log.h',
        'hadoop_table_operations.h',
        'hadoop_tables.h',
        'iceberg_hadoop_export.h',
    ],
    subdir: 'iceberg/catalog/hadoop',
)
