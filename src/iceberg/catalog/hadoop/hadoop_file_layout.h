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
#include <string>
#include <string_view>

#include "iceberg/catalog/hadoop/iceberg_hadoop_export.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"

/// \file iceberg/catalog/hadoop/hadoop_file_layout.h
/// \brief Pure helpers for the HadoopCatalog on-disk layout.
///
/// HadoopCatalog stores metadata directly on the filesystem in a fixed shape:
/// <warehouse>/<ns_level_1>/.../<ns_level_n>/<table>/metadata/v{N}.metadata.json
///                                                /metadata/version-hint.text
///                                                /metadata/_lock (optional)
///                                                /data/...
///
/// Filenames mirror Apache Iceberg's Java HadoopTableOperations exactly so
/// readers and writers across JVM/Python/C++ remain interoperable.

namespace iceberg::hadoop {

/// \brief Metadata file compression codec recognised by HadoopCatalog.
///
/// `kNone` writes `v{N}.metadata.json`; `kGzip` writes `v{N}.metadata.json.gz`;
/// `kZstd` writes `v{N}.metadata.json.zstd`. Java's table property
/// `write.metadata.compression-codec` selects between these on commit.
enum class MetadataCompressionCodec : uint8_t { kNone, kGzip, kZstd };

/// \brief Parse a codec name as recognised by the Java table property
/// `write.metadata.compression-codec`. Case-insensitive. Returns
/// `kInvalidArgument` if the name is unknown.
ICEBERG_HADOOP_EXPORT Result<MetadataCompressionCodec> ParseMetadataCompressionCodec(
    std::string_view name);

/// \brief Return the canonical name for a codec ("none", "gzip", "zstd").
ICEBERG_HADOOP_EXPORT std::string_view MetadataCompressionCodecName(
    MetadataCompressionCodec codec);

/// \brief Validate a single namespace level: must be non-empty and contain
/// no '/' characters. Java rejects `/` in namespace levels because of the
/// filesystem mapping; cpp follows suit.
ICEBERG_HADOOP_EXPORT Status ValidateNamespaceLevel(std::string_view level);

/// \brief Validate every level in a namespace.
ICEBERG_HADOOP_EXPORT Status ValidateNamespace(const Namespace& ns);

/// \brief Validate a table identifier: non-empty name, no '/' in name, plus
/// per-level namespace validation.
ICEBERG_HADOOP_EXPORT Status ValidateTableIdentifier(const TableIdentifier& identifier);

/// \brief Build the on-disk directory for a namespace. For an empty namespace
/// the warehouse root is returned (with trailing slashes stripped). Multi-level
/// namespaces become nested directories joined by '/'.
ICEBERG_HADOOP_EXPORT Result<std::string> NamespaceDir(std::string_view warehouse,
                                                       const Namespace& ns);

/// \brief Build the on-disk directory for a table: `NamespaceDir / name`.
ICEBERG_HADOOP_EXPORT Result<std::string> TableDir(std::string_view warehouse,
                                                   const TableIdentifier& identifier);

/// \brief Return the `metadata/` subdirectory of a table directory.
ICEBERG_HADOOP_EXPORT std::string MetadataDir(std::string_view table_dir);

/// \brief Return the `data/` subdirectory of a table directory.
ICEBERG_HADOOP_EXPORT std::string DataDir(std::string_view table_dir);

/// \brief Return the bare metadata file name for a given version + codec
/// (e.g. `v3.metadata.json.gz`).
ICEBERG_HADOOP_EXPORT std::string MetadataFileName(int64_t version,
                                                   MetadataCompressionCodec codec);

/// \brief Full path to the versioned metadata file under a table directory.
ICEBERG_HADOOP_EXPORT std::string MetadataFilePath(std::string_view table_dir,
                                                   int64_t version,
                                                   MetadataCompressionCodec codec);

/// \brief Path to the `version-hint.text` pointer file under a table.
ICEBERG_HADOOP_EXPORT std::string VersionHintPath(std::string_view table_dir);

/// \brief Path to the optional `_lock` file under a table's metadata directory.
ICEBERG_HADOOP_EXPORT std::string LockFilePath(std::string_view table_dir);

/// \brief Path to the optional `namespace.properties` file used by Phase 5
/// (E1) namespace property persistence. Not consumed in the MVP but exposed
/// here so callers do not have to reinvent the layout convention.
ICEBERG_HADOOP_EXPORT std::string NamespacePropertiesPath(std::string_view ns_dir);

/// \brief Parsed components of a metadata filename.
struct MetadataFileRef {
  int64_t version = 0;
  MetadataCompressionCodec codec = MetadataCompressionCodec::kNone;
};

/// \brief Parse a metadata file name produced by HadoopTableOperations.
/// Accepts: `v{N}.metadata.json`, `v{N}.metadata.json.gz`,
/// `v{N}.metadata.json.zstd`. Returns `kInvalidArgument` for anything else
/// (e.g. UUID-prefixed temp files left behind by a partial commit).
ICEBERG_HADOOP_EXPORT Result<MetadataFileRef> ParseMetadataFileName(
    std::string_view file_name);

}  // namespace iceberg::hadoop
