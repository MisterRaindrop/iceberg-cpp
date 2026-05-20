/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/catalog/hive/hive_catalog_properties.h"
#include "iceberg/catalog/hive/hive_utils.h"
#include "iceberg/catalog/hive/iceberg_hive_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/hive/hms_client.h
/// \brief Thin wrapper around the generated Hive Metastore Thrift client.
///
/// The header deliberately keeps Apache Thrift types out of the public
/// interface. All Thrift includes, transport / protocol setup and
/// generated-code references live in hms_client.cc behind a pImpl.

namespace iceberg::hive {

/// \brief A single host:port pair parsed from an HMS URI.
///
/// HMS URIs commonly take one of the forms:
///   * `thrift://host:port`        (Java HiveCatalog convention)
///   * `host:port`                 (iceberg-rust HmsCatalog convention)
///   * comma-separated list of either form for HA failover
///
/// `port` defaults to 9083 (Hive's well-known metastore port) when the
/// caller omits an explicit port.
struct ICEBERG_HIVE_EXPORT HmsEndpoint {
  std::string host;
  int port = 9083;
};

/// \brief Parse an HMS URI string into one or more endpoints.
///
/// Each comma-separated segment is treated as an independent endpoint;
/// surrounding whitespace and an optional `thrift://` scheme prefix are
/// stripped. Returns an InvalidArgument error if any segment fails to
/// produce a non-empty host and a port within (0, 65535].
ICEBERG_HIVE_EXPORT Result<std::vector<HmsEndpoint>> ParseHmsUris(std::string_view uri);

/// \brief A live connection to a Hive Metastore over Thrift.
///
/// The class encapsulates the TSocket / TTransport / TProtocol /
/// generated-client triple required by Apache Thrift's C++ runtime.
/// Construction goes through `Connect`, which performs URI parsing,
/// transport selection and an explicit `transport->open()` so that
/// configuration errors surface as `iceberg::Error` rather than C++
/// exceptions.
///
/// Thrift-call wrappers (namespace / table CRUD) are added in
/// follow-up commits (C10 / C11).
class ICEBERG_HIVE_EXPORT HmsClient {
 public:
  ~HmsClient();

  HmsClient(const HmsClient&) = delete;
  HmsClient& operator=(const HmsClient&) = delete;
  HmsClient(HmsClient&&) = delete;
  HmsClient& operator=(HmsClient&&) = delete;

  /// \brief Connect to the Hive Metastore described by `config`.
  ///
  /// The first endpoint listed in `config.Uri()` is used; HA failover
  /// to subsequent endpoints is left to a future commit.
  static Result<std::unique_ptr<HmsClient>> Connect(const HiveCatalogProperties& config);

  /// \name Database (namespace) operations
  /// Wrappers around the HMS Thrift `*_database` calls. Each method
  /// catches the specific Thrift exception subclasses and forwards
  /// them to the factories in hive_errors.h, so callers see typed
  /// `iceberg::Error` rather than raw C++ exceptions.
  /// @{

  /// \brief List the names of every database visible to this HMS.
  Result<std::vector<std::string>> GetAllDatabases();

  /// \brief Load a single database by name.
  ///
  /// Returns `kNoSuchNamespace` when the database does not exist.
  Result<HiveDatabase> GetDatabase(std::string_view name);

  /// \brief Create the database described by `database`.
  ///
  /// Returns `kAlreadyExists` when a database with the same name is
  /// already present.
  Status CreateDatabase(const HiveDatabase& database);

  /// \brief Drop a database by name. `cascade` is forwarded directly to
  /// HMS; when false, dropping a non-empty database returns
  /// `kNotAllowed`.
  Status DropDatabase(std::string_view name, bool cascade);

  /// \brief Replace the database with `name` by `database`.
  Status AlterDatabase(std::string_view name, const HiveDatabase& database);

  /// @}

  /// \name Table operations
  /// Wrappers around the HMS Thrift `*_table` calls. The conversion
  /// between `HiveTable` and the generated Thrift `Table` lives in
  /// hms_client.cc so Thrift types never leak into the public API.
  /// @{

  /// \brief List the names of every table in `db_name`.
  Result<std::vector<std::string>> GetAllTables(std::string_view db_name);

  /// \brief Load a single table by (database, table) name pair.
  ///
  /// Returns `kNoSuchTable` when the table does not exist.
  Result<HiveTable> GetTable(std::string_view db_name, std::string_view table_name);

  /// \brief Create the table described by `table`.
  Status CreateTable(const HiveTable& table);

  /// \brief Drop the table identified by `db_name` and `table_name`.
  ///
  /// `delete_data` is forwarded to HMS; iceberg_hive normally passes
  /// false because data file lifecycle is managed via Iceberg snapshot
  /// expiry rather than HMS.
  Status DropTable(std::string_view db_name, std::string_view table_name,
                   bool delete_data);

  /// \brief Replace the existing table at (`db_name`, `table_name`)
  /// with `new_table`. Used both for renames (when `new_table.db_name`
  /// or `new_table.table_name` differs from the source) and for in-place
  /// updates (parameter / location / columns) via the commit path.
  Status AlterTable(std::string_view db_name, std::string_view table_name,
                    const HiveTable& new_table);

  /// @}

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  explicit HmsClient(std::unique_ptr<Impl> impl);
};

}  // namespace iceberg::hive
