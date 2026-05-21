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

/// \brief Filesystem-backed TableOperations.
///
/// H08 introduces `Refresh()`. CreateTable / Commit / cleanup are introduced
/// in subsequent commits.
class ICEBERG_HADOOP_EXPORT HadoopTableOperations {
 public:
  HadoopTableOperations(std::shared_ptr<FileIO> file_io, std::string table_dir);

  /// \brief Reload the current metadata for the table.
  ///
  /// Returns the latest committed `TableMetadata` along with the absolute
  /// location of the metadata file it was loaded from. Reports
  /// `kNoSuchTable` when neither `version-hint.text` nor any
  /// `vN.metadata.json[.codec]` file is present.
  Result<std::shared_ptr<TableMetadata>> Refresh();

  /// \brief Location of the metadata file most recently returned by
  /// `Refresh()`. Empty before the first call.
  const std::string& current_metadata_location() const { return current_location_; }

  /// \brief Version number associated with `current_metadata_location`.
  /// Returns 0 before the first refresh.
  int64_t current_version() const { return current_version_; }

  /// \brief Table directory this operations object was constructed against.
  const std::string& table_dir() const { return table_dir_; }

 private:
  std::shared_ptr<FileIO> file_io_;
  std::string table_dir_;
  std::string current_location_;
  int64_t current_version_ = 0;
};

}  // namespace iceberg::hadoop
