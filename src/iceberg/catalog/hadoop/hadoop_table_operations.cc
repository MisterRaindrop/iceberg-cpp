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
  // `v{N+1}.metadata.json` file via atomic rename and only then update
  // version-hint.text, so observing hint=H guarantees v{H} exists on
  // disk. Listing first and reading the hint second can otherwise see a
  // listing snapshot taken before the writer's rename, but a hint
  // updated after both rename and hint write -- the reader then has
  // hint=N+1 with no v{N+1} in its stale listing and erroneously
  // surfaces kNoSuchTable.
  auto hint = ReadVersionHint(file_io, table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto versioned, ListVersionedMetadata(file_io, table_dir));

  if (hint.has_value()) {
    pointer.version = *hint;
  } else {
    int64_t best = -1;
    for (const auto& entry : versioned) {
      if (entry.version > best) {
        best = entry.version;
      }
    }
    if (best < 0) {
      return NoSuchTable("No metadata files found under '{}'.", MetadataDir(table_dir));
    }
    pointer.version = best;
    pointer.from_listdir_fallback = true;
  }

  for (auto& entry : versioned) {
    if (entry.version == pointer.version) {
      pointer.location = std::move(entry.location);
      return pointer;
    }
  }

  // Hint pointed at a version not present in our listing. Either the
  // hint races with a write that hasn't published the file yet (a
  // protocol violation, since writers rename the metadata file before
  // updating the hint), or our listing snapshot was taken before the
  // metadata rename landed. Re-list once and try again -- the writer's
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

constexpr std::string_view kWriteMetadataLocation = "write.metadata.location";

// Walk <table>/metadata/ and delete the oldest v{N}.metadata.json[.codec]
// files, keeping the newest `previous_versions_max` plus the current one.
// Mirrors the outcome of Java's
// `CatalogUtil.deleteRemovedMetadataFiles` for the HadoopCatalog layout
// without requiring metadata_log tracking inside HadoopTableOperations.
// Failures are surfaced via LogWarning -- the commit itself already
// succeeded, but operators need visibility into stuck GC.
void PruneOldMetadataFiles(FileIO& file_io, std::string_view table_dir,
                           int64_t current_version, int32_t previous_versions_max) {
  if (previous_versions_max <= 0) {
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
  if (updated.properties.configs().contains(std::string(kWriteMetadataLocation))) {
    return InvalidArgument(
        "Hadoop path-based tables cannot set '{}'; the metadata directory is fixed "
        "under the table location.",
        kWriteMetadataLocation);
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

  // (5) Pick the codec from the updated metadata's table properties. The
  // filename suffix follows the codec so a `.gz` file decodes correctly on
  // the next Refresh.
  const int64_t next_version = current.version + 1;
  ICEBERG_ASSIGN_OR_RAISE(auto codec, ResolveCommitCodec(updated));
  const std::string target = MetadataFilePath(table_dir_, next_version, codec);

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
  const std::string hint = VersionHintPath(table_dir_);
  const std::string hint_tmp =
      std::format("{}.tmp.{}", hint, Uuid::GenerateV7().ToString());
  const std::string payload = std::format("{}\n", next_version);
  if (auto write_status = file_io_->WriteFile(hint_tmp, payload);
      !write_status.has_value()) {
    std::ignore = file_io_->DeleteFile(target);
    return write_status;
  }

  auto rename_status = file_io_->Rename(hint_tmp, hint, /*overwrite=*/true);
  if (!rename_status.has_value()) {
    // (9) Recovery: the rename failed. Check whether some other writer (or
    // an earlier crashed run) already advanced the hint to our new version
    // -- if so, we landed despite the error.
    auto recheck = ResolveCurrentMetadata(*file_io_, table_dir_);
    if (recheck.has_value() && recheck->version == next_version) {
      std::ignore = file_io_->DeleteFile(hint_tmp);
      current_location_ = target;
      current_version_ = next_version;
      return {};
    }
    // Otherwise treat as a true failure: clean up our v{N+1} file and the
    // tmp hint, return kCommitFailed so the Transaction can retry.
    std::ignore = file_io_->DeleteFile(target);
    std::ignore = file_io_->DeleteFile(hint_tmp);
    return CommitFailed(
        "HadoopTableOperations::Commit: rename of version-hint.text failed and the "
        "hint did not advance: {}.",
        rename_status.error().message);
  }

  current_location_ = target;
  current_version_ = next_version;

  // (10) Release the lock now -- the commit has landed and any subsequent
  // bookkeeping (metadata GC) is best-effort and need not block other
  // writers waiting on the lock.
  guard.Dismiss();
  if (auto rel = lock_manager_->Release(table_dir_, owner_id_); !rel.has_value()) {
    LogWarning(std::format("HadoopCatalog: lock release failed for '{}': {}", table_dir_,
                           rel.error().message));
  }

  // (11) Commit-time stale metadata cleanup, honouring the Java table
  // properties `write.metadata.delete-after-commit.enabled` (default false)
  // and `write.metadata.previous-versions-max` (default 100). Runs outside
  // the lock so a long delete loop on a deep history does not stall other
  // writers; failures are logged because the commit itself already
  // succeeded.
  const bool delete_after_commit =
      updated.properties.Get(TableProperties::kMetadataDeleteAfterCommitEnabled);
  if (delete_after_commit) {
    const int32_t previous_versions_max =
        updated.properties.Get(TableProperties::kMetadataPreviousVersionsMax);
    PruneOldMetadataFiles(*file_io_, table_dir_, next_version, previous_versions_max);
  }
  return {};
}

}  // namespace iceberg::hadoop
