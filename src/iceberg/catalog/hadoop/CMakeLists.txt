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

set(ICEBERG_HADOOP_SOURCES
    hadoop_catalog.cc
    hadoop_catalog_properties.cc
    hadoop_file_layout.cc
    hadoop_lock_manager.cc
    hadoop_table_operations.cc
    hadoop_tables.cc)

set(ICEBERG_HADOOP_STATIC_BUILD_INTERFACE_LIBS)
set(ICEBERG_HADOOP_SHARED_BUILD_INTERFACE_LIBS)
set(ICEBERG_HADOOP_STATIC_INSTALL_INTERFACE_LIBS)
set(ICEBERG_HADOOP_SHARED_INSTALL_INTERFACE_LIBS)

list(APPEND ICEBERG_HADOOP_STATIC_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_static>,iceberg_static,iceberg_shared>")
list(APPEND ICEBERG_HADOOP_SHARED_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_shared>,iceberg_shared,iceberg_static>")
list(APPEND
     ICEBERG_HADOOP_STATIC_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_static>,iceberg::iceberg_static,iceberg::iceberg_shared>"
)
list(APPEND
     ICEBERG_HADOOP_SHARED_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_shared>,iceberg::iceberg_shared,iceberg::iceberg_static>"
)

add_iceberg_lib(iceberg_hadoop
                SOURCES
                ${ICEBERG_HADOOP_SOURCES}
                SHARED_LINK_LIBS
                ${ICEBERG_HADOOP_SHARED_BUILD_INTERFACE_LIBS}
                STATIC_LINK_LIBS
                ${ICEBERG_HADOOP_STATIC_BUILD_INTERFACE_LIBS}
                STATIC_INSTALL_INTERFACE_LIBS
                ${ICEBERG_HADOOP_STATIC_INSTALL_INTERFACE_LIBS}
                SHARED_INSTALL_INTERFACE_LIBS
                ${ICEBERG_HADOOP_SHARED_INSTALL_INTERFACE_LIBS})

iceberg_install_all_headers(iceberg/catalog/hadoop)
