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

#include "iceberg/catalog/hive/hive_catalog.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/catalog/hive/hive_utils.h"
#include "iceberg/catalog/hive/hms_client.h"
#include "iceberg/util/macros.h"

namespace iceberg::hive {

namespace {

constexpr std::string_view kNotImplementedMessage =
    "HiveCatalog method is not yet implemented; "
    "see the iceberg-cpp HiveCatalog roadmap (Phase 1 / Phase 2 commits).";

}  // namespace

HiveCatalog::HiveCatalog(HiveCatalogProperties config, std::unique_ptr<HmsClient> client)
    : config_(std::move(config)),
      name_(config_.Get(HiveCatalogProperties::kName)),
      client_(std::move(client)) {}

HiveCatalog::~HiveCatalog() = default;

Result<std::shared_ptr<HiveCatalog>> HiveCatalog::Make(
    const HiveCatalogProperties& config) {
  ICEBERG_ASSIGN_OR_RAISE(auto client, HmsClient::Connect(config));
  return std::shared_ptr<HiveCatalog>(new HiveCatalog(config, std::move(client)));
}

std::string_view HiveCatalog::name() const { return name_; }

Status HiveCatalog::CreateNamespace(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_ASSIGN_OR_RAISE(auto database, ConvertToHiveDatabase(ns, properties));
  return client_->CreateDatabase(database);
}

Result<std::vector<Namespace>> HiveCatalog::ListNamespaces(const Namespace& ns) const {
  // HMS has a flat namespace model. iceberg-rust returns an empty list
  // whenever a parent is supplied; do the same here so the catalog
  // behaves consistently across implementations.
  if (!ns.levels.empty()) {
    return std::vector<Namespace>{};
  }
  ICEBERG_ASSIGN_OR_RAISE(auto names, client_->GetAllDatabases());
  std::vector<Namespace> namespaces;
  namespaces.reserve(names.size());
  for (auto& db_name : names) {
    namespaces.push_back(Namespace{.levels = {std::move(db_name)}});
  }
  return namespaces;
}

Result<std::unordered_map<std::string, std::string>> HiveCatalog::GetNamespaceProperties(
    const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  ICEBERG_ASSIGN_OR_RAISE(auto database, client_->GetDatabase(ns.levels[0]));
  return ConvertFromHiveDatabase(database).properties;
}

Status HiveCatalog::DropNamespace(const Namespace& ns) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  // iceberg-rust mirrors Java's behaviour by dropping with cascade=false;
  // HMS surfaces "namespace not empty" as InvalidOperationException,
  // which hive_errors maps to ErrorKind::kNotAllowed.
  return client_->DropDatabase(ns.levels[0], /*cascade=*/false);
}

Result<bool> HiveCatalog::NamespaceExists(const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  auto database = client_->GetDatabase(ns.levels[0]);
  if (database.has_value()) {
    return true;
  }
  if (database.error().kind == ErrorKind::kNoSuchNamespace) {
    return false;
  }
  return std::unexpected(database.error());
}

Status HiveCatalog::UpdateNamespaceProperties(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
    const std::unordered_set<std::string>& removals) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  ICEBERG_ASSIGN_OR_RAISE(auto database, client_->GetDatabase(ns.levels[0]));

  // Reconstruct the property map, apply removals first then updates, and
  // hand the result back to ConvertToHiveDatabase so reserved keys are
  // re-applied to the dedicated HiveDatabase fields.
  auto namespace_view = ConvertFromHiveDatabase(database);
  auto& properties = namespace_view.properties;
  for (const auto& key : removals) {
    properties.erase(key);
  }
  for (const auto& [key, value] : updates) {
    properties[key] = value;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto altered, ConvertToHiveDatabase(ns, properties));
  return client_->AlterDatabase(ns.levels[0], altered);
}

Result<std::vector<TableIdentifier>> HiveCatalog::ListTables(
    const Namespace& /*ns*/) const {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::shared_ptr<Table>> HiveCatalog::CreateTable(
    const TableIdentifier& /*identifier*/, const std::shared_ptr<Schema>& /*schema*/,
    const std::shared_ptr<PartitionSpec>& /*spec*/,
    const std::shared_ptr<SortOrder>& /*order*/, const std::string& /*location*/,
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::shared_ptr<Table>> HiveCatalog::UpdateTable(
    const TableIdentifier& /*identifier*/,
    const std::vector<std::unique_ptr<TableRequirement>>& /*requirements*/,
    const std::vector<std::unique_ptr<TableUpdate>>& /*updates*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::shared_ptr<Transaction>> HiveCatalog::StageCreateTable(
    const TableIdentifier& /*identifier*/, const std::shared_ptr<Schema>& /*schema*/,
    const std::shared_ptr<PartitionSpec>& /*spec*/,
    const std::shared_ptr<SortOrder>& /*order*/, const std::string& /*location*/,
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<bool> HiveCatalog::TableExists(const TableIdentifier& /*identifier*/) const {
  return NotImplemented("{}", kNotImplementedMessage);
}

Status HiveCatalog::DropTable(const TableIdentifier& /*identifier*/, bool /*purge*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Status HiveCatalog::RenameTable(const TableIdentifier& /*from*/,
                                const TableIdentifier& /*to*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::shared_ptr<Table>> HiveCatalog::LoadTable(
    const TableIdentifier& /*identifier*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::shared_ptr<Table>> HiveCatalog::RegisterTable(
    const TableIdentifier& /*identifier*/,
    const std::string& /*metadata_file_location*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

}  // namespace iceberg::hive
