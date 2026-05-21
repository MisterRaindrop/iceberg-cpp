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
#include <unordered_set>

#include "iceberg/catalog.h"
#include "iceberg/catalog/hadoop/hadoop_catalog_properties.h"
#include "iceberg/catalog/hadoop/hadoop_lock_manager.h"
#include "iceberg/catalog/hadoop/iceberg_hadoop_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/hadoop/hadoop_catalog.h
/// \brief HadoopCatalog: file-system based Iceberg catalog (no metastore).

namespace iceberg::hadoop {

/// \brief File-system based Iceberg catalog.
///
/// HadoopCatalog stores all catalog state directly in the warehouse directory
/// tree: namespaces are directories, tables are subdirectories with a
/// `metadata/` folder, and `version-hint.text` points at the current metadata
/// version. Commit uses an atomic rename of `version-hint.text` for CAS.
///
/// The API mirrors Apache Iceberg's Java `HadoopCatalog` class. Behaviours
/// that Java explicitly rejects (e.g. namespace properties, RenameTable) are
/// rejected here as well, returning `iceberg::ErrorKind::kNotSupported`.
class ICEBERG_HADOOP_EXPORT HadoopCatalog
    : public Catalog,
      public std::enable_shared_from_this<HadoopCatalog> {
 public:
  ~HadoopCatalog() override;

  HadoopCatalog(const HadoopCatalog&) = delete;
  HadoopCatalog& operator=(const HadoopCatalog&) = delete;
  HadoopCatalog(HadoopCatalog&&) = delete;
  HadoopCatalog& operator=(HadoopCatalog&&) = delete;

  /// \brief Construct a HadoopCatalog instance.
  ///
  /// \param name Catalog name. Used as an identifier for log lines and lock
  ///             entity prefixes; does not need to be unique across processes.
  /// \param file_io Required FileIO that backs the warehouse. Callers may
  ///                construct this directly via the FileIO registry or rely on
  ///                a future helper that picks the right backend by scheme.
  /// \param config Catalog properties (see HadoopCatalogProperties).
  ///
  /// Validates that `warehouse` is set and that `file_io` is non-null. Emits a
  /// runtime warning to stderr when the warehouse scheme is `s3://`, because
  /// the HadoopCatalog commit protocol depends on filesystem atomic rename
  /// semantics that object stores do not provide.
  static Result<std::shared_ptr<HadoopCatalog>> Make(std::string_view name,
                                                     std::shared_ptr<FileIO> file_io,
                                                     HadoopCatalogProperties config);

  std::string_view name() const override;

  // -- Catalog interface (stubs in H02; implemented in subsequent commits). --

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

  /// \brief Accessor for the resolved properties; primarily for tests.
  const HadoopCatalogProperties& config() const { return config_; }

 private:
  HadoopCatalog(std::string name, std::shared_ptr<FileIO> file_io,
                HadoopCatalogProperties config,
                std::shared_ptr<LockManager> lock_manager);

  std::string name_;
  std::shared_ptr<FileIO> file_io_;
  HadoopCatalogProperties config_;
  std::shared_ptr<LockManager> lock_manager_;
};

}  // namespace iceberg::hadoop
