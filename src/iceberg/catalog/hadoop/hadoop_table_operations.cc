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
#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_properties.h"
#include "iceberg/util/gzip_internal.h"
#include "iceberg/util/macros.h"
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

Result<int64_t> ParseDecimalInt64(std::string_view text, std::string_view source) {
  if (text.empty()) {
    return Invalid("Empty version segment in {}.", source);
  }
  int64_t value = 0;
  for (char c : text) {
    if (c < '0' || c > '9') {
      return Invalid("Non-numeric character '{}' in {} version '{}'", c, source, text);
    }
    value = (value * 10) + (c - '0');
  }
  return value;
}

}  // namespace

Result<int64_t> ReadVersionHint(FileIO& file_io, std::string_view table_dir) {
  const std::string hint_path = VersionHintPath(table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto content,
                          file_io.ReadFile(hint_path, /*length=*/std::nullopt));
  TrimAsciiWhitespace(content);
  ICEBERG_ASSIGN_OR_RAISE(auto version, ParseDecimalInt64(content, "version-hint.text"));
  return version;
}

Result<int64_t> FindLatestMetadataVersion(FileIO& file_io, std::string_view table_dir) {
  const std::string metadata_dir = MetadataDir(table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto entries, file_io.ListDir(metadata_dir));
  int64_t best = -1;
  for (const auto& entry : entries) {
    if (entry.is_directory) {
      continue;
    }
    std::string_view name = entry.location;
    auto slash = name.find_last_of('/');
    if (slash != std::string_view::npos) {
      name.remove_prefix(slash + 1);
    }
    auto parsed = ParseMetadataFileName(name);
    if (!parsed.has_value()) {
      continue;
    }
    if (parsed->version > best) {
      best = parsed->version;
    }
  }
  if (best < 0) {
    return NoSuchTable("No metadata files found under '{}'.", metadata_dir);
  }
  return best;
}

Result<ResolvedMetadataPointer> ResolveCurrentMetadata(FileIO& file_io,
                                                       std::string_view table_dir) {
  ResolvedMetadataPointer pointer;

  auto hint = ReadVersionHint(file_io, table_dir);
  if (hint.has_value()) {
    pointer.version = *hint;
  } else {
    // Hint missing or corrupt: scan the metadata directory.
    ICEBERG_ASSIGN_OR_RAISE(auto scanned, FindLatestMetadataVersion(file_io, table_dir));
    pointer.version = scanned;
    pointer.from_listdir_fallback = true;
  }

  // The hint gives us the version; the file extension is decided by whichever
  // codec the writer chose. Probe the three accepted suffixes in turn.
  const std::string metadata_dir = MetadataDir(table_dir);
  for (auto codec : {MetadataCompressionCodec::kNone, MetadataCompressionCodec::kGzip,
                     MetadataCompressionCodec::kZstd}) {
    std::string candidate = metadata_dir + "/" + MetadataFileName(pointer.version, codec);
    ICEBERG_ASSIGN_OR_RAISE(auto exists, file_io.Exists(candidate));
    if (exists) {
      pointer.location = std::move(candidate);
      return pointer;
    }
  }
  return NoSuchTable("Metadata file v{}.metadata.json[.gz|.zstd] not found under '{}'.",
                     pointer.version, metadata_dir);
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

// Resolve the codec the writer should use for `metadata`. Defaults to
// `none` when the property is unset.
Result<MetadataCompressionCodec> ResolveCommitCodec(const TableMetadata& metadata) {
  const auto codec_name =
      metadata.properties.Get<std::string>(TableProperties::kMetadataCompression);
  return ParseMetadataCompressionCodec(codec_name);
}

// Walk <table>/metadata/ and delete the oldest v{N}.metadata.json[.codec]
// files, keeping the newest `previous_versions_max` plus the current one.
// Mirrors the outcome of Java's
// `CatalogUtil.deleteRemovedMetadataFiles` for the HadoopCatalog layout
// without requiring metadata_log tracking inside HadoopTableOperations.
void PruneOldMetadataFiles(FileIO& file_io, std::string_view table_dir,
                           int64_t current_version, int32_t previous_versions_max) {
  if (previous_versions_max <= 0) {
    return;
  }
  const std::string metadata_dir = MetadataDir(table_dir);
  auto entries = file_io.ListDir(metadata_dir);
  if (!entries.has_value()) {
    return;
  }
  std::vector<std::pair<int64_t, std::string>> versioned;
  versioned.reserve(entries->size());
  for (const auto& entry : *entries) {
    if (entry.is_directory) {
      continue;
    }
    std::string_view name = entry.location;
    auto slash = name.find_last_of('/');
    if (slash != std::string_view::npos) {
      name.remove_prefix(slash + 1);
    }
    auto parsed = ParseMetadataFileName(name);
    if (!parsed.has_value()) {
      continue;
    }
    versioned.emplace_back(parsed->version, entry.location);
  }
  // Sort by version ascending; we keep the newest `previous_versions_max`.
  std::ranges::sort(versioned,
                    [](const auto& a, const auto& b) { return a.first < b.first; });
  // Always retain the current version and the previous_versions_max files
  // immediately below it. Anything older is fair game.
  const int64_t cutoff_version = current_version - previous_versions_max;
  for (const auto& [version, path] : versioned) {
    if (version >= cutoff_version || version == current_version) {
      continue;
    }
    std::ignore = file_io.DeleteFile(path);
  }
}

// Serialise + (optionally) compress `metadata` and write it to `location` via
// FileIO. Mirrors the behaviour the Java HadoopTableOperations achieves via
// TableMetadataParser.Codec.
Status WriteMetadataWithCodec(FileIO& file_io, const std::string& location,
                              const TableMetadata& metadata,
                              MetadataCompressionCodec codec) {
  auto json = ToJson(metadata);
  ICEBERG_ASSIGN_OR_RAISE(std::string body, ToJsonString(json));
  switch (codec) {
    case MetadataCompressionCodec::kNone:
      break;
    case MetadataCompressionCodec::kGzip: {
      GZipCompressor compressor;
      ICEBERG_RETURN_UNEXPECTED(compressor.Init());
      ICEBERG_ASSIGN_OR_RAISE(body, compressor.Compress(body));
      break;
    }
    case MetadataCompressionCodec::kZstd:
      return NotSupported(
          "write.metadata.compression-codec=zstd is recognised by the codec helpers "
          "but zstd serialisation is not yet implemented in iceberg-cpp; use "
          "'none' or 'gzip'.");
  }
  return file_io.WriteFile(location, body);
}

// RAII guard that releases a held lock when leaving the Commit scope. The
// guard is no-op when no lock manager is wired in (e.g. during unit tests
// that exercise the algorithm without acquiring) which keeps the body of
// Commit() linear and easy to reason about.
struct LockReleaseGuard {
  LockManager* manager = nullptr;
  std::string entity_id;
  std::string owner_id;
  bool active = false;

  ~LockReleaseGuard() {
    if (active && manager != nullptr) {
      std::ignore = manager->Release(entity_id, owner_id);
    }
  }
};

}  // namespace

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
  LockReleaseGuard guard{.manager = lock_manager_.get(),
                         .entity_id = table_dir_,
                         .owner_id = owner_id_,
                         .active = true};

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
    return CommitFailed(
        "HadoopTableOperations::Commit: rename to '{}' failed (lost CAS race): {}",
        target, rename_to_target.error().message);
  }

  // (8) Update version-hint.text atomically: write tmp -> delete old -> rename.
  const std::string hint = VersionHintPath(table_dir_);
  const std::string hint_tmp = hint + ".tmp";
  const std::string payload = std::format("{}\n", next_version);
  if (auto write_status = file_io_->WriteFile(hint_tmp, payload);
      !write_status.has_value()) {
    std::ignore = file_io_->DeleteFile(target);
    return write_status;
  }
  // Delete the old hint best-effort; an explicit DeleteFile failure is ok
  // because the subsequent Rename(overwrite=false) below would also fail and
  // we handle that.
  std::ignore = file_io_->DeleteFile(hint);

  auto rename_status = file_io_->Rename(hint_tmp, hint, /*overwrite=*/false);
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

  // (9.5) Commit-time stale metadata cleanup, honouring the Java table
  // properties `write.metadata.delete-after-commit.enabled` (default false)
  // and `write.metadata.previous-versions-max` (default 100). Errors are
  // swallowed because the commit itself already succeeded -- losing a
  // GC pass is recoverable; failing the commit is not.
  const bool delete_after_commit =
      updated.properties.Get(TableProperties::kMetadataDeleteAfterCommitEnabled);
  if (delete_after_commit) {
    const int32_t previous_versions_max =
        updated.properties.Get(TableProperties::kMetadataPreviousVersionsMax);
    PruneOldMetadataFiles(*file_io_, table_dir_, next_version, previous_versions_max);
  }
  // (10) Lock is released by `guard` at scope exit.
  return {};
}

}  // namespace iceberg::hadoop
