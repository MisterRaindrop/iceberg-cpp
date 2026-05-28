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

#include <cstdint>
#include <memory>
#include <string>

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/catalog/hadoop/hadoop_lock_manager.h"
#include "iceberg/catalog/hadoop/iceberg_hadoop_export.h"
#include "iceberg/file_io.h"
#include "iceberg/result.h"
#include "iceberg/table_metadata.h"

/// \file iceberg/catalog/hadoop/hadoop_table_operations.h
/// \brief Filesystem-backed TableOperations for HadoopCatalog.

namespace iceberg::hadoop {

/// \brief Result of resolving the current metadata pointer for a table.
struct ResolvedMetadataPointer {
  /// \brief Absolute location of the resolved metadata file.
  std::string location;
  /// \brief Version number embedded in the metadata file name.
  int64_t version = 0;
  /// \brief True when the version was discovered by scanning the metadata
  /// directory (i.e. `version-hint.text` was missing or unreadable). Callers
  /// that want strict consistency can choose to fail when this is true.
  bool from_listdir_fallback = false;
};

/// \brief Read `version-hint.text` and return the integer it contains.
ICEBERG_HADOOP_EXPORT Result<int64_t> ReadVersionHint(FileIO& file_io,
                                                      std::string_view table_dir);

/// \brief Discover the most recent metadata version by listing
/// `<table>/metadata/`. Used as a fallback when `version-hint.text` is
/// missing or unparseable.
ICEBERG_HADOOP_EXPORT Result<int64_t> FindLatestMetadataVersion(
    FileIO& file_io, std::string_view table_dir);

/// \brief Locate the metadata pointer for a HadoopCatalog table.
///
/// Reads `version-hint.text` first. If the hint is missing, falls back to
/// scanning `metadata/` for the highest `vN.metadata.json[.codec]` file
/// (mirroring Java's `HadoopTableOperations.findVersion`). Returns
/// kNoSuchTable when the metadata directory has no usable version files.
ICEBERG_HADOOP_EXPORT Result<ResolvedMetadataPointer> ResolveCurrentMetadata(
    FileIO& file_io, std::string_view table_dir);

/// \brief Resolve the metadata compression codec the writer should use for
/// `metadata`. Reads `write.metadata.compression-codec` from the table
/// properties; defaults to `kNone`.
ICEBERG_HADOOP_EXPORT Result<MetadataCompressionCodec> ResolveCommitCodec(
    const TableMetadata& metadata);

/// \brief Serialise `metadata` (JSON + optional gzip) and return the bytes.
/// Used by both the commit path and the initial-v1 publish path so on-disk
/// encoding stays consistent.
ICEBERG_HADOOP_EXPORT Result<std::string> EncodeMetadataWithCodec(
    const TableMetadata& metadata, MetadataCompressionCodec codec);

/// \brief Serialise `metadata` (JSON + optional gzip) and write the result to
/// `location` via `file_io`. Thin wrapper over `EncodeMetadataWithCodec`.
ICEBERG_HADOOP_EXPORT Status WriteMetadataWithCodec(FileIO& file_io,
                                                    const std::string& location,
                                                    const TableMetadata& metadata,
                                                    MetadataCompressionCodec codec);

/// \brief Filesystem-backed TableOperations.
class ICEBERG_HADOOP_EXPORT HadoopTableOperations {
 public:
  /// \brief Construct without a lock manager. Refresh() works; Commit() will
  /// return kInvalidArgument until a lock manager is wired in via the
  /// catalog-provided constructor.
  HadoopTableOperations(std::shared_ptr<FileIO> file_io, std::string table_dir);

  /// \brief Construct with a lock manager so Commit() can serialize writers.
  HadoopTableOperations(std::shared_ptr<FileIO> file_io, std::string table_dir,
                        std::shared_ptr<LockManager> lock_manager, std::string owner_id);

  /// \brief Reload the current metadata for the table.
  ///
  /// Returns the latest committed `TableMetadata` along with the absolute
  /// location of the metadata file it was loaded from. Reports
  /// `kNoSuchTable` when neither `version-hint.text` nor any
  /// `vN.metadata.json[.codec]` file is present.
  Result<std::shared_ptr<TableMetadata>> Refresh();

  /// \brief Commit `updated` if and only if `base`'s metadata pointer is
  /// still the current one.
  ///
  /// Mirrors Java HadoopTableOperations with iceberg-cpp's additional ABA
  /// guard. Numbered to match the in-file step comments:
  ///   1. Reject any change to `metadata.location` (Java requires
  ///      filesystem tables to stay at their original path).
  ///   2. Reject metadata that overrides `write.metadata.path` or
  ///      `write.data.path` -- both would orphan data on
  ///      `DropTable(purge=true)`.
  ///   3. Acquire the LockManager. Acquire timeout -> kCommitFailed.
  ///   4. Re-resolve the current pointer; if it has drifted from `base`,
  ///      return kCommitFailed so `iceberg::Transaction` can retry.
  ///   4b. ABA guard: re-read the FULL current metadata under the lock
  ///       and require its `table_uuid` to match `base.table_uuid` (a
  ///       concurrent drop+recreate would land the same version with a
  ///       different uuid). Uuid-less bases are refused outright.
  ///   5. Pick a target filename based on the table's
  ///      `write.metadata.compression-codec` property.
  ///   5b. Codec-independent CAS scan: refuse if ANY v{next_version}.*
  ///       file already exists (defense against two writers picking
  ///       different codecs and both succeeding on rename).
  ///   6/7. Write to a UUID-named temp file, then atomically
  ///        `Rename(overwrite=false)` to the canonical
  ///        `v{N}.metadata.json[.codec]`. Rename failure with
  ///        kAlreadyExists -> kCommitFailed; any other backend error is
  ///        surfaced as-is so Transaction does not retry forever against
  ///        an infrastructure failure.
  ///   8. THE METADATA RENAME IS THE COMMIT POINT. From here on,
  ///      `ResolveCurrentMetadata` will discover the new version via the
  ///      `max(hint, listdir-max)` fallback even without a hint update.
  ///      Update `version-hint.text` via write-tmp + atomic
  ///      `Rename(overwrite=true)`. A hint write/rename failure is
  ///      logged as a WARNING only -- the commit has already landed and
  ///      rolling it back would break readers that have observed the
  ///      new metadata file.
  ///   9. Honour `write.metadata.delete-after-commit.enabled` (default
  ///      false) by pruning v{N - previous-versions-max} and older
  ///      metadata files. Runs UNDER the commit lock so a concurrent
  ///      drop+recreate at this path cannot land new files before GC
  ///      finishes scanning the directory.
  ///   10. Release the lock in a `finally`-like cleanup.
  Status Commit(const TableMetadata& base, const TableMetadata& updated);

  /// \brief Location of the metadata file most recently returned by
  /// `Refresh()` or written by `Commit()`. Empty before either call.
  const std::string& current_metadata_location() const { return current_location_; }

  /// \brief Version number associated with `current_metadata_location`.
  /// Returns 0 before any refresh/commit succeeds.
  int64_t current_version() const { return current_version_; }

  /// \brief Table directory this operations object was constructed against.
  const std::string& table_dir() const { return table_dir_; }

 private:
  std::shared_ptr<FileIO> file_io_;
  std::string table_dir_;
  std::shared_ptr<LockManager> lock_manager_;
  std::string owner_id_;
  std::string current_location_;
  int64_t current_version_ = 0;
};

}  // namespace iceberg::hadoop
