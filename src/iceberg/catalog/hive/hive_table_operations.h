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
/// `metadata_location` + optional HMS EXCLUSIVE table lock). On any
/// commit failure the freshly-written metadata file is best-effort
/// deleted so the warehouse stays free of orphan files.
class ICEBERG_HIVE_EXPORT HiveTableOperations {
 public:
  /// \param lock_enabled If true, `Commit` wraps the
  /// `GetTable -> AlterTable` critical section with an HMS EXCLUSIVE
  /// TABLE-level lock for extra safety on top of `metadata_location`
  /// CAS. Defaults to false; the CAS alone is sufficient for
  /// single-writer correctness but high-concurrency deployments may
  /// want the extra guard. Controlled by
  /// `HiveCatalogProperties::kLockEnabled`.
  HiveTableOperations(HmsClient* client, std::shared_ptr<FileIO> file_io,
                      TableIdentifier identifier, bool lock_enabled = false);

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
  /// `base.metadata_location`. On mismatch returns `kCommitFailed`
  /// (callers should retry via `iceberg::Transaction`). On any
  /// failure (CAS, AlterTable, or thrown Thrift exception) the
  /// freshly-written metadata file is best-effort deleted so the
  /// warehouse stays free of orphans.
  ///
  /// Returns the location of the newly-written metadata file on
  /// success so the caller can fold it into a refreshed snapshot.
  Result<std::string> Commit(const HiveTableMetadataSnapshot& base,
                             const TableMetadata& new_metadata);

  const TableIdentifier& identifier() const { return identifier_; }

 private:
  HmsClient* client_;
  std::shared_ptr<FileIO> file_io_;
  TableIdentifier identifier_;
  bool lock_enabled_;
};

}  // namespace iceberg::hive
