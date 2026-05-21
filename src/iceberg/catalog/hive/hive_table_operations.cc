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

#include "iceberg/catalog/hive/hive_schema.h"
#include "iceberg/catalog/hive/hive_utils.h"
#include "iceberg/catalog/hive/hms_client.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg::hive {

HiveTableOperations::HiveTableOperations(
    HmsClient* client, std::shared_ptr<FileIO> file_io, TableIdentifier identifier,
    bool lock_enabled, HmsLockOptions lock_options,
    std::optional<HiveCatalogProperties> heartbeat_config)
    : client_(client),
      file_io_(std::move(file_io)),
      identifier_(std::move(identifier)),
      lock_enabled_(lock_enabled),
      lock_options_(lock_options),
      heartbeat_config_(std::move(heartbeat_config)) {}

Result<HiveTableMetadataSnapshot> HiveTableOperations::Refresh() {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier_.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier_.Validate());

  ICEBERG_ASSIGN_OR_RAISE(auto hive_table,
                          client_->GetTable(identifier_.ns.levels[0], identifier_.name));
  ICEBERG_RETURN_UNEXPECTED(ValidateIcebergTable(identifier_, hive_table.parameters));
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
  std::unique_ptr<HmsLockHeartbeat> heartbeat;
  if (lock_enabled_) {
    auto lock_or_error =
        client_->LockExclusive(identifier_.ns.levels[0], identifier_.name, lock_options_);
    if (!lock_or_error.has_value()) {
      cleanup();
      return std::unexpected(lock_or_error.error());
    }
    lock_handle = *lock_or_error;
    // Keep the lock alive against HMS's server-side txn.timeout. Failure
    // to start the heartbeat is non-fatal -- the CAS alone protects
    // correctness on short commits -- so we ignore the error and proceed.
    if (heartbeat_config_.has_value() && lock_options_.heartbeat_interval_ms > 0) {
      auto hb = HmsLockHeartbeat::Start(*heartbeat_config_, lock_handle.lock_id,
                                        lock_options_.heartbeat_interval_ms);
      if (hb.has_value()) {
        heartbeat = std::move(*hb);
      }
    }
  }
  const auto release_lock = [&] {
    // Stop the heartbeat thread BEFORE releasing the lock so the worker
    // never races against a freshly-reclaimed lock_id.
    if (heartbeat) {
      heartbeat->Stop();
    }
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

  // Re-check the table_type marker even though Refresh already validated it.
  // A concurrent Hive DDL (e.g., ALTER TABLE UNSET TBLPROPERTIES) could have
  // stripped `table_type=ICEBERG` between Refresh and now; without this guard
  // we would happily overwrite an unrelated Hive table's row with Iceberg
  // metadata_location parameters.
  if (auto validate = ValidateIcebergTable(identifier_, current.parameters);
      !validate.has_value()) {
    release_lock();
    cleanup();
    return std::unexpected(validate.error());
  }

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
  // Keep the HMS column list in sync with the committed schema so engines
  // that introspect HMS (Hive `DESCRIBE`, Spark / Trino schema discovery)
  // see the post-evolution columns. Mirrors Java's
  // `HiveTableOperations.commitToExistingTable()` calling
  // `setCols(HiveSchemaUtil.convert(newMetadata.schema()))`. Propagate
  // any failure from `Schema()` (e.g., dangling current_schema_id) so we
  // do not silently leave HMS columns out of sync with metadata_location.
  {
    auto new_schema = new_metadata.Schema();
    if (!new_schema.has_value()) {
      release_lock();
      cleanup();
      return std::unexpected(new_schema.error());
    }
    if (*new_schema != nullptr) {
      auto columns = SchemaToHiveColumns(**new_schema);
      if (!columns.has_value()) {
        release_lock();
        cleanup();
        return std::unexpected(columns.error());
      }
      current.columns = std::move(*columns);
    }
  }
  auto alter_status =
      client_->AlterTable(identifier_.ns.levels[0], identifier_.name, current);
  if (!alter_status.has_value()) {
    // AlterTable may have actually landed on HMS even though the Thrift
    // response was lost. Mirror Java's `checkCommitStatus` and re-read the
    // table once to classify the failure:
    //   * `metadata_location == new`  -> commit landed; treat as success.
    //   * `metadata_location == base` -> commit didn't land; retriable
    //                                    (`kCommitFailed`).
    //   * any other value             -> someone else committed; the new
    //                                    metadata file is now orphaned but
    //                                    we cannot safely delete it without
    //                                    proof, so surface
    //                                    `kCommitStateUnknown` (no retry).
    //   * GetTable -> kNoSuchTable    -> table was dropped concurrently;
    //                                    retriable (`kCommitFailed`).
    //   * GetTable -> any other error -> indeterminate; surface
    //                                    `kCommitStateUnknown`.
    auto verify = client_->GetTable(identifier_.ns.levels[0], identifier_.name);
    if (!verify.has_value()) {
      release_lock();
      if (verify.error().kind == ErrorKind::kNoSuchTable) {
        cleanup();
        return CommitFailed(
            "HMS AlterTable failed and target table was dropped concurrently; "
            "retry the transaction (msg={}).",
            alter_status.error().message);
      }
      // Don't cleanup -- we don't know whether AlterTable actually landed,
      // so deleting new_metadata_location could orphan a committed table.
      // Don't surface this as `kServiceUnavailable` either: that would
      // trigger `HmsClientPool::Run`'s reconnect-once contract, which
      // re-executes the enclosing UpdateTable lambda and could re-apply
      // mutations a second time on top of a committed-but-unverifiable
      // first attempt. The pool's broken socket self-heals on the next
      // caller's first RPC; surfacing `kCommitStateUnknown` here tells
      // `iceberg::Transaction` to stop retrying instead.
      return CommitStateUnknown(
          "HMS AlterTable failed and verification GetTable also failed; "
          "the commit may or may not have landed (alter={}, verify={}).",
          alter_status.error().message, verify.error().message);
    }
    auto verify_location = GetMetadataLocation(verify.value().parameters);
    if (verify_location.has_value() && *verify_location == new_metadata_location) {
      // Commit landed despite the exception; keep the metadata file.
      release_lock();
      return new_metadata_location;
    }
    release_lock();
    if (verify_location.has_value() && *verify_location == base.metadata_location) {
      cleanup();
      return CommitFailed(
          "HMS AlterTable failed for {}; retry the transaction (kind={}, msg={}).",
          identifier_.ToString(), static_cast<int>(alter_status.error().kind),
          alter_status.error().message);
    }
    // Indeterminate: a third committer landed something, or the table now
    // lacks metadata_location entirely. Don't delete our file because we
    // can't tell whether it is the live one.
    return CommitStateUnknown(
        "HMS AlterTable failed and HMS now reports an unexpected "
        "metadata_location for {}; treat the commit as undefined "
        "(alter={}, observed={}).",
        identifier_.ToString(), alter_status.error().message,
        verify_location.has_value() ? *verify_location : std::string("<missing>"));
  }
  release_lock();
  return new_metadata_location;
}

}  // namespace iceberg::hive
