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

#include "iceberg/catalog/hive/hive_table_operations.h"

#include <format>
#include <optional>
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/hive/hive_utils.h"
#include "iceberg/catalog/hive/hms_client.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg::hive {

HiveTableOperations::HiveTableOperations(HmsClient* client,
                                         std::shared_ptr<FileIO> file_io,
                                         TableIdentifier identifier, bool lock_enabled)
    : client_(client),
      file_io_(std::move(file_io)),
      identifier_(std::move(identifier)),
      lock_enabled_(lock_enabled) {}

Result<HiveTableMetadataSnapshot> HiveTableOperations::Refresh() {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier_.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier_.Validate());

  ICEBERG_ASSIGN_OR_RAISE(auto hive_table,
                          client_->GetTable(identifier_.ns.levels[0], identifier_.name));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_location,
                          GetMetadataLocation(hive_table.parameters));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_json,
                          file_io_->ReadFile(metadata_location, /*length=*/std::nullopt));

  nlohmann::json metadata_obj;
  try {
    metadata_obj = nlohmann::json::parse(metadata_json);
  } catch (const nlohmann::json::parse_error& e) {
    return JsonParseError("Failed to parse metadata at '{}': {}", metadata_location,
                          e.what());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, TableMetadataFromJson(metadata_obj));

  return HiveTableMetadataSnapshot{
      .metadata = std::shared_ptr<TableMetadata>(metadata.release()),
      .metadata_location = std::move(metadata_location),
  };
}

namespace {

std::string NewMetadataLocation(const TableMetadata& new_metadata,
                                std::size_t base_version) {
  // Mirror Java HiveTableOperations: <root>/metadata/<NNNNN>-<uuid>.metadata.json
  // where NNNNN is a 5-digit zero-padded version derived from the base
  // metadata_log length (the next version after `base`).
  return std::format("{}/metadata/{:05}-{}.metadata.json", new_metadata.location,
                     base_version + 1, Uuid::GenerateV4().ToString());
}

}  // namespace

Result<std::string> HiveTableOperations::Commit(const HiveTableMetadataSnapshot& base,
                                                const TableMetadata& new_metadata) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier_.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier_.Validate());
  if (!base.metadata) {
    return InvalidArgument("HiveTableOperations::Commit requires a base snapshot.");
  }

  // 1. Serialise the new metadata and write it to a fresh file.
  const std::string new_metadata_location =
      NewMetadataLocation(new_metadata, base.metadata->metadata_log.size());
  ICEBERG_ASSIGN_OR_RAISE(const auto json_str, ToJsonString(new_metadata));
  ICEBERG_RETURN_UNEXPECTED(file_io_->WriteFile(new_metadata_location, json_str));

  // 2-4. CAS via alter_table. Best-effort cleanup on every failure path
  // so the warehouse never accumulates orphan metadata files. When
  // `lock_enabled_` is true, wrap the GetTable -> AlterTable section in
  // an HMS EXCLUSIVE table-level lock; the CAS alone is sufficient for
  // single-writer correctness, but high-concurrency deployments may
  // prefer the explicit mutex.
  const auto cleanup = [&] { (void)file_io_->DeleteFile(new_metadata_location); };

  HmsClient::HmsLockHandle lock_handle;
  if (lock_enabled_) {
    auto lock_or_error =
        client_->LockExclusive(identifier_.ns.levels[0], identifier_.name);
    if (!lock_or_error.has_value()) {
      cleanup();
      return std::unexpected(lock_or_error.error());
    }
    lock_handle = *lock_or_error;
  }
  const auto release_lock = [&] {
    if (lock_handle.acquired()) {
      (void)client_->Unlock(lock_handle);
    }
  };

  auto current_or_error = client_->GetTable(identifier_.ns.levels[0], identifier_.name);
  if (!current_or_error.has_value()) {
    release_lock();
    cleanup();
    return std::unexpected(current_or_error.error());
  }
  HiveTable& current = current_or_error.value();

  auto current_location_or_error = GetMetadataLocation(current.parameters);
  if (!current_location_or_error.has_value()) {
    release_lock();
    cleanup();
    return std::unexpected(current_location_or_error.error());
  }
  if (*current_location_or_error != base.metadata_location) {
    release_lock();
    cleanup();
    return CommitFailed(
        "HMS metadata_location for {} changed from '{}' to '{}'; retry the "
        "transaction.",
        identifier_.ToString(), base.metadata_location, *current_location_or_error);
  }

  current.parameters[std::string(kMetadataLocationKey)] = new_metadata_location;
  current.parameters[std::string(kPreviousMetadataLocationKey)] = base.metadata_location;
  auto alter_status =
      client_->AlterTable(identifier_.ns.levels[0], identifier_.name, current);
  if (!alter_status.has_value()) {
    release_lock();
    cleanup();
    return std::unexpected(alter_status.error());
  }
  release_lock();
  return new_metadata_location;
}

}  // namespace iceberg::hive
