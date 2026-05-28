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
#include "iceberg/file_io.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"

/// \file iceberg/catalog/hadoop/hadoop_file_layout.h
/// \brief Pure helpers for the HadoopCatalog on-disk layout.
///
/// HadoopCatalog stores metadata directly on the filesystem in a fixed shape:
/// <warehouse>/<ns_level_1>/.../<ns_level_n>/<table>/metadata/v{N}.metadata.json
///                                                /metadata/version-hint.text
///                                                /data/...
///
/// With `lock-impl=file`, the file-based LockManager keeps its lock files
/// in a warehouse-shared directory rather than under each table:
/// <warehouse>/_iceberg_catalog_locks/<FNV1a64-hex>.lock
/// The lock root name is position-aware reserved (see `kLockRootDirName`),
/// so no user table or namespace can collide with it at the warehouse root.
///
/// Filenames mirror Apache Iceberg's Java HadoopTableOperations exactly so
/// readers and writers across JVM/Python/C++ remain interoperable.

namespace iceberg::hadoop {

/// \brief Name of the warehouse-level directory where `lock-impl=file`
/// stores its lock files (`<warehouse>/_iceberg_catalog_locks/`).
///
/// The name is reserved POSITION-AWARE: only the FIRST namespace level (or
/// a top-level table name) may not equal this value, because that is the
/// only depth where it would collide with the on-disk lock root. Nested
/// uses like `db._iceberg_catalog_locks` are legitimate namespaces and are
/// not rejected. The position-aware guard lives in the catalog layer
/// (HadoopCatalog::CreateNamespace / CreateTable / UpdateTable /
/// RegisterTable) and at the HadoopTables bare-path API boundary via
/// `RejectUnsafeTablePath`. The namespace read APIs (`NamespaceExists`,
/// `GetNamespaceProperties`, `ListNamespaces`, `ListTables`,
/// `DropNamespace`) treat the top-level lock root as not-a-namespace, and
/// `ListNamespaces({})` filters it out of warehouse-root listings.
/// `ValidateNamespaceLevel`/`ValidateTableIdentifier` do NOT carry the
/// reserved-name check, because they are level-agnostic and would
/// incorrectly forbid nested uses.
inline constexpr std::string_view kLockRootDirName = "_iceberg_catalog_locks";

/// \brief Metadata file compression codec recognised by HadoopCatalog.
///
/// `kNone` writes `v{N}.metadata.json`; `kGzip` writes the core-canonical
/// `v{N}.gz.metadata.json` (`TableMetadataUtil::Codec::kGzipTableMetadataFileSuffix`).
/// `kZstd` is reachable as a codec name but the encoder side returns
/// `kNotSupported`; the file shape never lands on disk via this catalog.
/// Java's table property `write.metadata.compression-codec` selects between
/// these on commit. Parser accepts the legacy `v{N}.metadata.json.gz`
/// shape for compatibility with older writers.
enum class MetadataCompressionCodec : uint8_t { kNone, kGzip, kZstd };

/// \brief Parse a codec name as recognised by the Java table property
/// `write.metadata.compression-codec`. Case-insensitive. Returns
/// `kInvalidArgument` if the name is unknown.
ICEBERG_HADOOP_EXPORT Result<MetadataCompressionCodec> ParseMetadataCompressionCodec(
    std::string_view name);

/// \brief Return the canonical name for a codec ("none", "gzip", "zstd").
ICEBERG_HADOOP_EXPORT std::string_view MetadataCompressionCodecName(
    MetadataCompressionCodec codec);

/// \brief Reduce a warehouse string to a canonical form suitable for
/// re-use as the IO URI prefix.
///
/// The result is SAFE to re-parse as a URI: percent-decoding and
/// dot-segment collapsing are deliberately NOT performed here (decoding
/// then re-parsing would mis-resolve reserved characters like `#` /
/// `%2F`, and round-tripping `%252F` would collapse it to a path
/// separator). The function only:
///   1. Rejects URI warehouses carrying a raw `?` or `#` (arrow's URI
///      parser would truncate the path at the marker).
///   2. Rejects filesystem-root warehouses (`file:///`, `s3://`, `/`,
///      `///`): the trailing-slash strip below would corrupt URI roots
///      to bare schemes, and a bare `/` would collapse to an empty
///      prefix in `NamespaceDir`.
///   3. Strips trailing slashes so `file:///wh` and `file:///wh/`
///      collapse together.
///
/// Alias collapsing for lock identity (`%20` vs space, `.`/`..` segment
/// folding, `file://` vs literal path) is intentionally separate and
/// lives in `CanonicalLockKey`, whose output is used only for lock
/// identity and never for IO.
ICEBERG_HADOOP_EXPORT Result<std::string> CanonicalizeWarehouse(
    std::string_view warehouse);

/// \brief Produce a stable physical-identity string for `entity_id`,
/// used ONLY as a lock map key / file-lock hash input (never as an IO
/// URI). Percent-decodes the URI form, collapses `.`/`..` segments, and
/// strips a trailing slash, so that surface aliases of the same physical
/// directory -- `file:///tmp/wh`, `file:///tmp/./wh`,
/// `file:///tmp/my%20wh` vs `file:///tmp/my wh` -- map to a single lock
/// key. Literal (non-URI) paths are not percent-decoded.
ICEBERG_HADOOP_EXPORT std::string CanonicalLockKey(std::string_view entity_id);

/// \brief True if `location` uses one of the S3-family schemes
/// (`s3://`, `s3a://`, `s3n://`). NOTE: arrow-fs-s3 only accepts the
/// canonical `s3://`; HadoopCatalog::Make and HadoopTables reject the
/// JVM-only Hadoop aliases on the auto-detect path. This helper is still
/// useful for emitting non-atomic-warehouse warnings, scheme-based
/// branching, and for callers that have a custom FileIO that understands
/// the aliases via the `io-impl` override.
ICEBERG_HADOOP_EXPORT bool IsS3Scheme(std::string_view location);

/// \brief Return the leaf component of a slash-separated path. Returns the
/// whole input when there is no slash. Pure string operation; does not touch
/// the filesystem.
ICEBERG_HADOOP_EXPORT std::string_view Basename(std::string_view path);

/// \brief Reject a HadoopTables bare-path table location that the layout
/// cannot safely handle. The bare-path API has no warehouse/identifier
/// validation, so this guards its boundary against two hazards:
///   1. a URI carrying a raw `?` (query) or `#` (fragment) marker --
///      arrow's URI parser truncates the path there, so the resolved
///      directory would not match the literal string (see
///      `CanonicalizeWarehouse` for the same rule on warehouses);
///   2. a path whose leaf component is the reserved lock-root directory
///      name `_iceberg_catalog_locks` -- a `lock-impl=file` catalog stores
///      its lock files there, so creating/purging a table over it would
///      corrupt active file locks. Trailing slashes are stripped before
///      the leaf comparison so `.../_iceberg_catalog_locks/` is caught.
ICEBERG_HADOOP_EXPORT Status RejectUnsafeTablePath(std::string_view path,
                                                   std::string_view source);

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
/// (e.g. `v3.gz.metadata.json`).
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

/// \brief Parsed components of a metadata filename.
struct MetadataFileRef {
  int64_t version = 0;
  MetadataCompressionCodec codec = MetadataCompressionCodec::kNone;
};

/// \brief Parse a metadata file name produced by HadoopTableOperations.
///
/// Accepts the three shapes iceberg-cpp can read:
///   - `v{N}.metadata.json`           (uncompressed)
///   - `v{N}.gz.metadata.json`        (gzip, core-canonical)
///   - `v{N}.metadata.json.gz`        (gzip, legacy Hadoop writers)
///
/// zstd-suffixed files are DELIBERATELY rejected: the encoder side returns
/// `kNotSupported` for zstd, so a `v{N}...zstd` file in the metadata dir is
/// from an external writer iceberg-cpp's reader cannot decode. Accepting it
/// here would make `TableExists` true while `LoadTable` later failed mid
/// JSON parse -- worse than reporting "no such table" cleanly.
///
/// Returns `kInvalidArgument` for anything else (e.g. UUID-prefixed temp
/// files left behind by a partial commit).
ICEBERG_HADOOP_EXPORT Result<MetadataFileRef> ParseMetadataFileName(
    std::string_view file_name);

/// \brief Determine whether `dir_location` resolves to a HadoopCatalog table
/// directory.
///
/// A directory qualifies when it contains a `metadata/` subdirectory that has
/// at least one `v{N}.metadata.json[.codec]` file (matching the HadoopCatalog
/// commit layout). The check tolerates a missing or non-directory parent (in
/// which case the result is false). Any underlying FileIO failure other than
/// "not found" is propagated.
ICEBERG_HADOOP_EXPORT Result<bool> IsHadoopTableDir(FileIO& file_io,
                                                    std::string_view dir_location);

/// \brief True iff `path` is either equal to `parent_dir` or a strict
/// descendant of it (i.e. `parent_dir` followed by `/` then a non-empty
/// remainder). Both inputs are slash-stripped on the right before the
/// comparison so trailing slashes do not change the result.
///
/// CRITICAL: this is a COMPONENT-AWARE descendant test. A naive
/// `path.starts_with(parent_dir)` would say
/// `/wh/db/stats_backup` is under `/wh/db/stats`, which is wrong --
/// allowing such siblings to slip past a "path is inside the table dir"
/// guard would let `DropTable(purge=true)` silently leave externally
/// referenced files behind. Use this helper anywhere the catalog needs
/// to assert that an externally-supplied path stays within the table
/// (or warehouse) it claims.
ICEBERG_HADOOP_EXPORT bool IsPathInside(std::string_view path,
                                        std::string_view parent_dir);

/// \brief Like `IsPathInside`, but conservatively rejects paths that
/// contain traversal aliases or percent-encoded equivalents.
///
/// Used to check externally-sourced paths (e.g. `metadata.statistics[].path`,
/// `metadata.metadata_log[].metadata_file`) that come out of registered
/// metadata files. Such paths may contain `..` or `%2e%2e` segments that
/// the OS / Arrow URI parser would normalise BEFORE the IO call -- so
/// `file:///wh/db/t/../outside/x.puffin` and
/// `file:///wh/db/t/%2e%2e/outside/x.puffin` both resolve to
/// `file:///wh/db/outside/x.puffin` at IO time even though `IsPathInside`
/// would happily classify them as "inside `/wh/db/t`".
///
/// The check:
///   1. percent-decodes any `%HH` sequences in `path` (decoding any
///      hex pair -- not just `%2e`/`%2f` -- so re-encoded `..` survives);
///   2. rejects (returns false) any segment that resolves to `.` or `..`
///      (i.e. refuses to even try to canonicalise traversal-bearing
///      paths -- we expect statistics / metadata-log paths emitted by
///      well-behaved writers to be in canonical form already);
///   3. runs `IsPathInside(decoded_path, parent_dir)` for the final
///      descendant decision.
///
/// `parent_dir` is taken as-is (it is built internally from the warehouse
/// and validated identifiers and therefore contains no traversal aliases
/// or percent-encoding -- the identifier validator rejects `%`).
ICEBERG_HADOOP_EXPORT bool IsPathInsideNormalized(std::string_view path,
                                                  std::string_view parent_dir);

/// \brief Walk every proper prefix of `ns` (inclusive) and check that
/// the resulting directory is NOT a Hadoop table. Returns an error if
/// any ancestor is a table -- such a namespace/table would live INSIDE
/// an existing table's directory tree, and a later
/// `DropTable(purge=true)` on the ancestor table would recursively wipe
/// the descendant catalog entry.
///
/// `last_inclusive` selects whether the namespace itself is also
/// checked: pass true when checking CreateNamespace (the leaf would
/// land where the table currently lives); pass false when checking the
/// PARENT namespace of a new table (the table's own dir is checked
/// separately by IsHadoopTableDir at the caller).
ICEBERG_HADOOP_EXPORT Status RejectAncestorIsTable(FileIO& file_io,
                                                   std::string_view warehouse,
                                                   const Namespace& ns,
                                                   bool last_inclusive);

/// \brief True iff `dir_location` exists as a directory and contains a child
/// entry whose leaf name is not in `{"metadata", "data"}`.
///
/// HadoopCatalog uses this to refuse creating a table at a path that already
/// hosts a namespace -- a namespace's children are sub-namespaces or other
/// tables, so any unexpected leaf indicates the path is "occupied". Children
/// named `metadata` / `data` are tolerated so that a half-created table or a
/// table with the standard layout under the path is not flagged here (the
/// caller already runs IsHadoopTableDir to detect a completed table and the
/// locked recheck distinguishes "in-progress create" inside Commit).
///
/// Returns `false` when the directory does not exist or is not a directory.
ICEBERG_HADOOP_EXPORT Result<bool> HasNonTableInternalChildren(
    FileIO& file_io, std::string_view dir_location);

}  // namespace iceberg::hadoop
