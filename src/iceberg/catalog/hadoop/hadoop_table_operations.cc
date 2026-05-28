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

#include "iceberg/catalog/hadoop/hadoop_table_operations.h"

#include <format>
#include <string>
#include <string_view>
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/catalog/hadoop/hadoop_log.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_properties.h"
#include "iceberg/util/gzip_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"
#include "iceberg/util/uuid.h"

namespace iceberg::hadoop {

namespace {

// Trim leading and trailing ASCII whitespace from a string in-place.
void TrimAsciiWhitespace(std::string& s) {
  auto is_space = [](unsigned char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
  };
  while (!s.empty() && is_space(static_cast<unsigned char>(s.back()))) {
    s.pop_back();
  }
  size_t front = 0;
  while (front < s.size() && is_space(static_cast<unsigned char>(s[front]))) {
    ++front;
  }
  if (front > 0) {
    s.erase(0, front);
  }
}

}  // namespace

Result<int64_t> ReadVersionHint(FileIO& file_io, std::string_view table_dir) {
  const std::string hint_path = VersionHintPath(table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto content,
                          file_io.ReadFile(hint_path, /*length=*/std::nullopt));
  TrimAsciiWhitespace(content);
  ICEBERG_ASSIGN_OR_RAISE(auto version, StringUtils::ParseNumber<int64_t>(content));
  return version;
}

namespace {

// Cache the parsed metadata directory listing keyed by name so the resolver
// can pick the matching v{N} entry without re-listing. Returns a vector of
// (version, codec, location).
struct VersionedEntry {
  int64_t version = 0;
  MetadataCompressionCodec codec = MetadataCompressionCodec::kNone;
  std::string location;
};

Result<std::vector<VersionedEntry>> ListVersionedMetadata(FileIO& file_io,
                                                          std::string_view table_dir) {
  const std::string metadata_dir = MetadataDir(table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto entries, file_io.ListDir(metadata_dir));
  std::vector<VersionedEntry> out;
  out.reserve(entries.size());
  for (const auto& entry : entries) {
    if (entry.is_directory) {
      continue;
    }
    auto parsed = ParseMetadataFileName(Basename(entry.location));
    if (!parsed.has_value()) {
      continue;
    }
    out.push_back(
        {.version = parsed->version, .codec = parsed->codec, .location = entry.location});
  }
  return out;
}

}  // namespace

Result<int64_t> FindLatestMetadataVersion(FileIO& file_io, std::string_view table_dir) {
  ICEBERG_ASSIGN_OR_RAISE(auto versioned, ListVersionedMetadata(file_io, table_dir));
  int64_t best = -1;
  for (const auto& entry : versioned) {
    if (entry.version > best) {
      best = entry.version;
    }
  }
  if (best < 0) {
    return NoSuchTable("No metadata files found under '{}'.", MetadataDir(table_dir));
  }
  return best;
}

Result<ResolvedMetadataPointer> ResolveCurrentMetadata(FileIO& file_io,
                                                       std::string_view table_dir) {
  ResolvedMetadataPointer pointer;

  // Surface "table directory doesn't exist" as kNoSuchTable up front so
  // Catalog::LoadTable returns the documented error kind (arrow's ListDir
  // on a missing directory returns kIOError, which would otherwise leak).
  const std::string metadata_dir = MetadataDir(table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto md_exists, file_io.Exists(metadata_dir));
  if (!md_exists) {
    return NoSuchTable("Table at '{}' has no metadata directory.", table_dir);
  }

  // Read the version hint BEFORE listing. Writers publish the new
  // `v{N+1}.metadata.json` file via atomic rename and ONLY THEN update
  // version-hint.text -- the rename IS the commit point, so observing the
  // file is authoritative regardless of whether the subsequent hint update
  // succeeded. Reading the hint first ensures we don't see a listing taken
  // after a writer's rename but with a hint that's already been advanced
  // past our snapshot.
  auto hint = ReadVersionHint(file_io, table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto versioned, ListVersionedMetadata(file_io, table_dir));

  int64_t list_max = -1;
  for (const auto& entry : versioned) {
    if (entry.version > list_max) {
      list_max = entry.version;
    }
  }

  if (hint.has_value()) {
    // CRITICAL: pick max(hint, list_max). A writer that crashed between the
    // metadata rename (the commit point) and the hint update leaves
    // v{hint+1} on disk while the hint still reads `hint`. Trusting the
    // hint alone would mean (a) Refresh loads stale metadata even though a
    // newer committed version exists, and (b) the next commit's
    // rename(temp, v{hint+1}, overwrite=false) would fail with
    // AlreadyExists forever. The rename IS the CAS commit; trust it.
    pointer.version = list_max > *hint ? list_max : *hint;
    pointer.from_listdir_fallback = list_max > *hint;
  } else {
    if (list_max < 0) {
      return NoSuchTable("No metadata files found under '{}'.", MetadataDir(table_dir));
    }
    pointer.version = list_max;
    pointer.from_listdir_fallback = true;
  }

  for (auto& entry : versioned) {
    if (entry.version == pointer.version) {
      pointer.location = std::move(entry.location);
      return pointer;
    }
  }

  // Hint pointed at a version not present in our listing (this branch is
  // only reachable when list_max < hint, i.e. hint is ahead of what we
  // managed to list). Either the listing snapshot was taken before the
  // metadata rename landed, or a different writer raced the hint update
  // ahead of our list view. Re-list once and try again -- the writer's
  // rename has been visible by now if the hint update was.
  ICEBERG_ASSIGN_OR_RAISE(auto retry_versioned,
                          ListVersionedMetadata(file_io, table_dir));
  for (auto& entry : retry_versioned) {
    if (entry.version == pointer.version) {
      pointer.location = std::move(entry.location);
      return pointer;
    }
  }
  return NoSuchTable("Metadata file v{}.metadata.json[.gz|.zstd] not found under '{}'.",
                     pointer.version, MetadataDir(table_dir));
}

HadoopTableOperations::HadoopTableOperations(std::shared_ptr<FileIO> file_io,
                                             std::string table_dir)
    : file_io_(std::move(file_io)), table_dir_(std::move(table_dir)) {}

HadoopTableOperations::HadoopTableOperations(std::shared_ptr<FileIO> file_io,
                                             std::string table_dir,
                                             std::shared_ptr<LockManager> lock_manager,
                                             std::string owner_id)
    : file_io_(std::move(file_io)),
      table_dir_(std::move(table_dir)),
      lock_manager_(std::move(lock_manager)),
      owner_id_(std::move(owner_id)) {}

Result<std::shared_ptr<TableMetadata>> HadoopTableOperations::Refresh() {
  if (file_io_ == nullptr) {
    return InvalidArgument("HadoopTableOperations: FileIO is null.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto pointer, ResolveCurrentMetadata(*file_io_, table_dir_));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata,
                          TableMetadataUtil::Read(*file_io_, pointer.location));
  current_location_ = pointer.location;
  current_version_ = pointer.version;
  return std::shared_ptr<TableMetadata>(std::move(metadata));
}

namespace {

// Resolve the actual table-property key at use sites via
// TableProperties::kWriteMetadataLocation.key() ("write.metadata.path") so we
// stay in sync with the canonical definition.

// Walk <table>/metadata/ and delete the oldest v{N}.metadata.json[.codec]
// files, keeping the newest `previous_versions_max` plus the current one.
// Mirrors the outcome of Java's
// `CatalogUtil.deleteRemovedMetadataFiles` for the HadoopCatalog layout
// without requiring metadata_log tracking inside HadoopTableOperations.
// Failures are surfaced via LogWarning -- the commit itself already
// succeeded, but operators need visibility into stuck GC.
void PruneOldMetadataFiles(FileIO& file_io, std::string_view table_dir,
                           int64_t current_version, int32_t previous_versions_max) {
  // Java treats `write.metadata.previous-versions-max` as a non-negative cap
  // and accepts 0 as a valid "keep current only" setting. Negative values
  // are not standard but we treat them as "disable GC" so callers can opt
  // out without dropping the delete-after-commit flag.
  if (previous_versions_max < 0) {
    return;
  }
  auto versioned_result = ListVersionedMetadata(file_io, table_dir);
  if (!versioned_result.has_value()) {
    LogWarning(std::format("PruneOldMetadataFiles: ListDir failed for '{}': {}",
                           MetadataDir(table_dir), versioned_result.error().message));
    return;
  }
  auto versioned = std::move(*versioned_result);
  if (static_cast<int64_t>(versioned.size()) <= previous_versions_max + 1) {
    return;
  }
  std::ranges::sort(versioned,
                    [](const auto& a, const auto& b) { return a.version < b.version; });
  const int64_t cutoff_version = current_version - previous_versions_max;
  for (const auto& entry : versioned) {
    if (entry.version >= cutoff_version || entry.version == current_version) {
      continue;
    }
    if (auto del = file_io.DeleteFile(entry.location); !del.has_value()) {
      LogWarning(std::format("PruneOldMetadataFiles: DeleteFile('{}') failed: {}",
                             entry.location, del.error().message));
    }
  }
}

}  // namespace

Result<MetadataCompressionCodec> ResolveCommitCodec(const TableMetadata& metadata) {
  const auto codec_name =
      metadata.properties.Get<std::string>(TableProperties::kMetadataCompression);
  return ParseMetadataCompressionCodec(codec_name);
}

Result<std::string> EncodeMetadataWithCodec(const TableMetadata& metadata,
                                            MetadataCompressionCodec codec) {
  auto json = ToJson(metadata);
  ICEBERG_ASSIGN_OR_RAISE(std::string body, ToJsonString(json));
  switch (codec) {
    case MetadataCompressionCodec::kNone:
      return body;
    case MetadataCompressionCodec::kGzip: {
      GZipCompressor compressor;
      ICEBERG_RETURN_UNEXPECTED(compressor.Init());
      return compressor.Compress(body);
    }
    case MetadataCompressionCodec::kZstd:
      return NotSupported(
          "write.metadata.compression-codec=zstd is recognised by the codec helpers "
          "but zstd serialisation is not yet implemented in iceberg-cpp; use "
          "'none' or 'gzip'.");
  }
  return body;
}

Status WriteMetadataWithCodec(FileIO& file_io, const std::string& location,
                              const TableMetadata& metadata,
                              MetadataCompressionCodec codec) {
  ICEBERG_ASSIGN_OR_RAISE(auto body, EncodeMetadataWithCodec(metadata, codec));
  return file_io.WriteFile(location, body);
}

Status HadoopTableOperations::Commit(const TableMetadata& base,
                                     const TableMetadata& updated) {
  if (file_io_ == nullptr) {
    return InvalidArgument("HadoopTableOperations: FileIO is null.");
  }
  if (lock_manager_ == nullptr) {
    return InvalidArgument(
        "HadoopTableOperations::Commit requires a LockManager. Construct with the "
        "4-arg constructor or use HadoopCatalog::UpdateTable.");
  }

  // (1) Path-based tables cannot relocate. This is a Java hard constraint and
  // protects callers from accidentally orphaning their data directory.
  if (base.location != updated.location) {
    return InvalidArgument("Hadoop path-based tables cannot be relocated ('{}' != '{}').",
                           base.location, updated.location);
  }
  // (2) Path-based tables cannot redirect metadata to an external location;
  // doing so would break version-hint.text resolution.
  if (updated.properties.configs().contains(
          TableProperties::kWriteMetadataLocation.key())) {
    return InvalidArgument(
        "Hadoop path-based tables cannot set '{}'; the metadata directory is fixed "
        "under the table location.",
        TableProperties::kWriteMetadataLocation.key());
  }
  // (2b) HadoopCatalog::DropTable(purge=true) deletes the table dir tree
  // only, so allowing `write.data.path` would orphan the actual data on
  // a successful purge -- a silent contract violation. Reject the property
  // here too; CreateTable / RegisterTable enforce the same on their paths.
  if (updated.properties.configs().contains(TableProperties::kWriteDataLocation.key())) {
    return InvalidArgument(
        "Hadoop path-based tables cannot set '{}'; data outside the table dir "
        "would be orphaned by DropTable(purge=true).",
        TableProperties::kWriteDataLocation.key());
  }

  // (3) Acquire the lock.
  ICEBERG_ASSIGN_OR_RAISE(auto acquired, lock_manager_->Acquire(table_dir_, owner_id_));
  if (!acquired) {
    return CommitFailed(
        "HadoopTableOperations::Commit: failed to acquire lock for '{}' within the "
        "configured timeout.",
        table_dir_);
  }
  LockReleaseGuard guard(lock_manager_.get(), table_dir_, owner_id_);

  // (4) CAS check: ensure no concurrent writer has advanced the pointer.
  ICEBERG_ASSIGN_OR_RAISE(auto current, ResolveCurrentMetadata(*file_io_, table_dir_));
  if (current.version != current_version_) {
    return CommitFailed(
        "HadoopTableOperations::Commit: stale base. current version {} != cached {}.",
        current.version, current_version_);
  }
  // (4b) ABA guard. The version number alone does NOT identify a table
  // instance: a concurrent DropTable + CreateTable at the same path
  // produces a fresh table whose current version is ALSO v1 but with a
  // different table UUID. The update's requirements (including AssertUUID)
  // were validated against the OLD metadata BEFORE we took the lock, so
  // without this check we would publish v2 derived from the old
  // generation onto the new table, silently clobbering it. Re-read the
  // full current metadata under the lock and require its UUID to still
  // match the base we built from.
  //
  // A base with an EMPTY uuid would defeat the guard: a drop+recreate of
  // another uuid-less generation also lands at the same version with an
  // empty uuid, so `"" == ""` would wrongly pass. TableMetadata::Make
  // assigns a uuid and RegisterTable rejects uuid-less metadata, so this
  // only fires for externally crafted state -- refuse it rather than
  // commit blind.
  if (base.table_uuid.empty()) {
    return CommitFailed(
        "HadoopTableOperations::Commit: base metadata for '{}' has no table-uuid; "
        "the drop+recreate ABA guard cannot operate without one. Refresh and retry.",
        table_dir_);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto current_metadata,
                          TableMetadataUtil::Read(*file_io_, current.location));
  if (current_metadata->table_uuid != base.table_uuid) {
    return CommitFailed(
        "HadoopTableOperations::Commit: table at '{}' was replaced (uuid '{}' != "
        "base uuid '{}'); a concurrent drop+recreate happened. Refresh and retry.",
        table_dir_, current_metadata->table_uuid, base.table_uuid);
  }

  // (5) Pick the codec from the updated metadata's table properties. The
  // filename suffix follows the codec so a `.gz` file decodes correctly on
  // the next Refresh.
  const int64_t next_version = current.version + 1;
  ICEBERG_ASSIGN_OR_RAISE(auto codec, ResolveCommitCodec(updated));
  const std::string target = MetadataFilePath(table_dir_, next_version, codec);

  // (5b) Codec-independent CAS: rename(overwrite=false) alone is not enough
  // when two concurrent writers pick different codecs -- one would publish
  // `v{N+1}.metadata.json` and the other `v{N+1}.metadata.json.gz`, BOTH
  // renames would succeed, and Refresh would arbitrarily pick whichever
  // file the listing surfaces first, silently losing one commit. Under the
  // lock we additionally reject if ANY file matching v{next_version}.*
  // already exists, making the CAS independent of the chosen codec.
  ICEBERG_ASSIGN_OR_RAISE(auto existing, ListVersionedMetadata(*file_io_, table_dir_));
  for (const auto& entry : existing) {
    if (entry.version == next_version) {
      return CommitFailed(
          "HadoopTableOperations::Commit: lost CAS race to v{}: a metadata file "
          "at this version (codec-independent) already exists at '{}'.",
          next_version, entry.location);
    }
  }

  // (6/7) Write to a UUID-named temp file first, then atomically rename to
  // the canonical `v{N}.metadata.json[.codec]`. This is the same trick that
  // Java's HadoopTableOperations.renameToFinal uses; without it, a crashed
  // commit could leave an orphan v{N+1} file that permanently blocks the
  // next writer's fail-if-exists check. Orphan UUID-named files do NOT
  // match the ParseMetadataFileName pattern (which requires `v{N}` prefix),
  // so listdir-based recovery ignores them. The rename(overwrite=false) is
  // our CAS primitive -- if another writer raced us to v{N+1}, the rename
  // fails and we surface kCommitFailed without clobbering their commit.
  const std::string metadata_dir = MetadataDir(table_dir_);
  const std::string temp_path = std::format("{}/{}-v{}.metadata.json.tmp", metadata_dir,
                                            Uuid::GenerateV7().ToString(), next_version);
  ICEBERG_RETURN_UNEXPECTED(WriteMetadataWithCodec(*file_io_, temp_path, updated, codec));

  auto rename_to_target = file_io_->Rename(temp_path, target, /*overwrite=*/false);
  if (!rename_to_target.has_value()) {
    std::ignore = file_io_->DeleteFile(temp_path);
    // Only the "destination already exists" failure means a concurrent
    // commit won the CAS race -- that one IS retryable, so surface it
    // as kCommitFailed for Transaction::Commit. Permission denied,
    // NotSupported, generic IOError etc. must propagate as themselves,
    // otherwise Transaction would re-run the commit attempt forever
    // against an infrastructure failure that will never resolve.
    if (rename_to_target.error().kind == ErrorKind::kAlreadyExists) {
      return CommitFailed(
          "HadoopTableOperations::Commit: lost CAS race to '{}': another writer "
          "already published the next version.",
          target);
    }
    return rename_to_target;
  }

  // (8) Update version-hint.text via write-tmp + atomic-replace. The lock
  // serialises writers, so a single atomic rename(overwrite=true) is enough;
  // delete-then-rename would leave a brief window with no hint at all, and a
  // crash there would force every subsequent Refresh into the listdir
  // fallback path.
  //
  // CRITICAL: the rename of `target` above IS the commit point. From that
  // moment on `ResolveCurrentMetadata` will surface v{next_version} via
  // its listdir-max fallback (which prefers max(hint, listdir-max) over
  // the hint alone). Deleting `target` on a later hint failure would roll
  // back a committed publish and break any concurrent reader that has
  // already observed it. Treat hint failures here as soft warnings.
  current_location_ = target;
  current_version_ = next_version;
  const std::string hint = VersionHintPath(table_dir_);
  const std::string hint_tmp =
      std::format("{}.tmp.{}", hint, Uuid::GenerateV7().ToString());
  const std::string payload = std::format("{}\n", next_version);
  if (auto write_status = file_io_->WriteFile(hint_tmp, payload);
      !write_status.has_value()) {
    LogWarning(std::format(
        "HadoopCatalog: committed v{} at '{}' but version-hint.text write failed: "
        "{}; Refresh will recover via listdir fallback.",
        next_version, target, write_status.error().message));
  } else if (auto rename_status = file_io_->Rename(hint_tmp, hint, /*overwrite=*/true);
             !rename_status.has_value()) {
    std::ignore = file_io_->DeleteFile(hint_tmp);
    LogWarning(
        std::format("HadoopCatalog: committed v{} at '{}' but version-hint.text rename "
                    "failed: {}; Refresh will recover via listdir fallback.",
                    next_version, target, rename_status.error().message));
  }

  // (10) Commit-time stale metadata cleanup, honouring the Java table
  // properties `write.metadata.delete-after-commit.enabled` (default false)
  // and `write.metadata.previous-versions-max` (default 100). Runs UNDER
  // THE COMMIT LOCK so a concurrent DropTable + CreateTable on this path
  // (a different "generation" of the table) cannot land its own files
  // before our GC scans the directory. If we released early, a drop +
  // recreate sequence would let GC delete the new generation's v1
  // metadata as if it were an old version of our committed table.
  const bool delete_after_commit =
      updated.properties.Get(TableProperties::kMetadataDeleteAfterCommitEnabled);
  if (delete_after_commit) {
    const int32_t previous_versions_max =
        updated.properties.Get(TableProperties::kMetadataPreviousVersionsMax);
    PruneOldMetadataFiles(*file_io_, table_dir_, next_version, previous_versions_max);
  }
  // (11) Release the lock now that GC is done; failures here are
  // post-CAS bookkeeping (logged, not propagated -- the commit landed).
  guard.Dismiss();
  if (auto rel = lock_manager_->Release(table_dir_, owner_id_); !rel.has_value()) {
    LogWarning(std::format("HadoopCatalog: lock release failed for '{}': {}", table_dir_,
                           rel.error().message));
  }
  return {};
}

}  // namespace iceberg::hadoop
