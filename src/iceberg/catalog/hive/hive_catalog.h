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
#include <unordered_set>

#include "iceberg/catalog.h"
#include "iceberg/catalog/hive/hive_catalog_properties.h"
#include "iceberg/catalog/hive/iceberg_hive_export.h"
#include "iceberg/file_io.h"
#include "iceberg/result.h"

namespace iceberg::hive {
class HmsClient;
}  // namespace iceberg::hive

/// \file iceberg/catalog/hive/hive_catalog.h
/// \brief HiveCatalog implementation for talking to a Hive Metastore (HMS).

namespace iceberg::hive {

/// \brief Catalog implementation backed by a Hive Metastore.
///
/// Talks to HMS over Thrift via `HmsClient` and supports:
///   * full namespace CRUD (create / list / properties / update / drop / exists)
///   * table CRUD: `CreateTable` (writes initial metadata.json then registers in
///     HMS, rolling back the metadata file on any HMS failure), `LoadTable`,
///     `UpdateTable` (Refresh → TableMetadataBuilder.Apply → CAS commit),
///     `DropTable(purge=false)`, `RenameTable`, `TableExists`, `RegisterTable`,
///     `ListTables`
///   * Commit path: write new metadata.json → `metadata_location` CAS via
///     `alter_table` → on mismatch return `kCommitFailed` so `iceberg::Transaction`
///     retries; failure paths best-effort delete the orphan metadata file
///   * Optional HMS `EXCLUSIVE` table-level lock around the commit when
///     `hive.lock-enabled=true`
///
/// `DropTable(purge=true)` and SASL/Kerberos authentication remain
/// `kNotImplemented`; see `mkdocs/docs/catalogs/hive.md` for the full status
/// matrix and remaining gaps.
class ICEBERG_HIVE_EXPORT HiveCatalog : public Catalog,
                                        public std::enable_shared_from_this<HiveCatalog> {
 public:
  ~HiveCatalog() override;

  HiveCatalog(const HiveCatalog&) = delete;
  HiveCatalog& operator=(const HiveCatalog&) = delete;
  HiveCatalog(HiveCatalog&&) = delete;
  HiveCatalog& operator=(HiveCatalog&&) = delete;

  /// \brief Construct a HiveCatalog from `config`.
  ///
  /// The MVP factory only stores the configuration; HMS connection setup
  /// is deferred to follow-up commits. Returns an error if the supplied
  /// configuration is missing required fields (currently: the URI).
  static Result<std::shared_ptr<HiveCatalog>> Make(const HiveCatalogProperties& config);

  std::string_view name() const override;

  Status CreateNamespace(
      const Namespace& ns,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<std::vector<Namespace>> ListNamespaces(const Namespace& ns) const override;

  Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
      const Namespace& ns) const override;

  Status DropNamespace(const Namespace& ns) override;

  Result<bool> NamespaceExists(const Namespace& ns) const override;

  Status UpdateNamespaceProperties(
      const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
      const std::unordered_set<std::string>& removals) override;

  Result<std::vector<TableIdentifier>> ListTables(const Namespace& ns) const override;

  Result<std::shared_ptr<Table>> CreateTable(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
      const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<std::shared_ptr<Table>> UpdateTable(
      const TableIdentifier& identifier,
      const std::vector<std::unique_ptr<TableRequirement>>& requirements,
      const std::vector<std::unique_ptr<TableUpdate>>& updates) override;

  Result<std::shared_ptr<Transaction>> StageCreateTable(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
      const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<bool> TableExists(const TableIdentifier& identifier) const override;

  Status DropTable(const TableIdentifier& identifier, bool purge) override;

  Status RenameTable(const TableIdentifier& from, const TableIdentifier& to) override;

  Result<std::shared_ptr<Table>> LoadTable(const TableIdentifier& identifier) override;

  Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier,
      const std::string& metadata_file_location) override;

 private:
  HiveCatalog(HiveCatalogProperties config, std::unique_ptr<HmsClient> client,
              std::shared_ptr<FileIO> file_io);

  HiveCatalogProperties config_;
  std::string name_;
  std::unique_ptr<HmsClient> client_;
  std::shared_ptr<FileIO> file_io_;
};

}  // namespace iceberg::hive
