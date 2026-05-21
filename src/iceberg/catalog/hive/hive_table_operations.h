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
#include <optional>
#include <string>

#include "iceberg/catalog/hive/hive_catalog_properties.h"
#include "iceberg/catalog/hive/hms_client.h"
#include "iceberg/catalog/hive/iceberg_hive_export.h"
#include "iceberg/file_io.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"

namespace iceberg::hive {

class HmsClient;

/// \brief Loaded TableMetadata together with the metadata file location.
///
/// Returned by `HiveTableOperations::Refresh` and passed back into
/// `HiveTableOperations::Commit` as the `base` so the CAS check has the
/// previous `metadata_location` to compare against.
struct ICEBERG_HIVE_EXPORT HiveTableMetadataSnapshot {
  std::shared_ptr<TableMetadata> metadata;
  std::string metadata_location;
};

/// \brief Per-table HMS read / write orchestrator.
///
/// Wraps the (HMS Thrift, FileIO) pair iceberg_hive needs to round-trip
/// table metadata. Exposes `Refresh()` (HMS GetTable + FileIO read of
/// the metadata.json) and `Commit()` (write new metadata.json + CAS via
/// `metadata_location` + optional HMS EXCLUSIVE table lock). Commit
/// failures that can be classified as definitively "did not land"
/// (`kCommitFailed`, `kNoSuchTable`) best-effort delete the freshly-
/// written metadata file so the warehouse stays free of orphans;
/// failures that leave the outcome indeterminate (`kCommitStateUnknown`)
/// intentionally KEEP the file because we cannot prove HMS no longer
/// points at it.
class ICEBERG_HIVE_EXPORT HiveTableOperations {
 public:
  /// \param lock_enabled If true, `Commit` wraps the
  /// `GetTable -> AlterTable` critical section with an HMS EXCLUSIVE
  /// TABLE-level lock for extra safety on top of `metadata_location`
  /// CAS. Defaults to false; the CAS alone is sufficient for
  /// single-writer correctness but high-concurrency deployments may
  /// want the extra guard. Controlled by
  /// `HiveCatalogProperties::kLockEnabled`.
  /// \param lock_options Polling intervals, acquire timeout, and
  /// heartbeat interval passed through to `HmsClient::LockExclusive`
  /// and `HmsLockHeartbeat`. Ignored when `lock_enabled` is false.
  /// Default-constructed values match Java's HiveCatalog defaults
  /// (50ms / 5s / 3min / 4min heartbeat).
  /// \param heartbeat_config When `lock_enabled` is true and this
  /// optional is engaged, `Commit` starts an `HmsLockHeartbeat` on its
  /// own dedicated connection (using the supplied config) and stops it
  /// before returning. When empty, no heartbeat is sent and the lock is
  /// bounded purely by HMS's `txn.timeout`. A copy is stored so the
  /// heartbeat does not depend on the lifetime of any external object.
  HiveTableOperations(
      HmsClient* client, std::shared_ptr<FileIO> file_io, TableIdentifier identifier,
      bool lock_enabled = false, HmsLockOptions lock_options = {},
      std::optional<HiveCatalogProperties> heartbeat_config = std::nullopt);

  /// \brief Load the table's current metadata from HMS + FileIO.
  ///
  /// Surfaces `kNoSuchTable` when the table is missing, `kNotFound`
  /// when the HMS row has no `metadata_location` parameter (i.e. the
  /// table is not Iceberg-managed), or `kJsonParseError` when the
  /// metadata file is malformed.
  Result<HiveTableMetadataSnapshot> Refresh();

  /// \brief Atomically replace `base` with `new_metadata`.
  ///
  /// Writes a fresh metadata.json under `new_metadata.location`, then
  /// CAS-checks HMS's current `metadata_location` parameter against
  /// `base.metadata_location` via an `alter_table` call. On mismatch
  /// returns `kCommitFailed` (callers should retry via
  /// `iceberg::Transaction`).
  ///
  /// AlterTable failures go through a three-state recovery
  /// (`checkCommitStatus`): if HMS now points at the new
  /// `metadata_location` the commit landed silently; if it still points
  /// at `base.metadata_location` the commit did not land and the file is
  /// cleaned up; any other observed state is surfaced as
  /// `kCommitStateUnknown` with the metadata file LEFT IN PLACE because
  /// we cannot prove HMS does not reference it.
  ///
  /// Returns the location of the newly-written metadata file on
  /// success so the caller can fold it into a refreshed snapshot.
  ///
  /// `mutation_attempted` (optional out-parameter) is set to `true` as
  /// soon as the AlterTable RPC is issued, regardless of outcome. The
  /// `HiveCatalog::UpdateTable` caller funnels this through
  /// `CommitStateUnknownOnTransportFailure` so a `kServiceUnavailable`
  /// that escapes after AlterTable was attempted is re-tagged
  /// `kCommitStateUnknown`, while a pre-AlterTable transport blip
  /// (Refresh / GetTable / pre-write) remains retriable. Leaving the
  /// pointer null preserves the original signal.
  Result<std::string> Commit(const HiveTableMetadataSnapshot& base,
                             TableMetadata& new_metadata,
                             bool* mutation_attempted = nullptr);

  const TableIdentifier& identifier() const { return identifier_; }

 private:
  HmsClient* client_;
  std::shared_ptr<FileIO> file_io_;
  TableIdentifier identifier_;
  bool lock_enabled_;
  HmsLockOptions lock_options_;
  std::optional<HiveCatalogProperties> heartbeat_config_;
};

}  // namespace iceberg::hive
