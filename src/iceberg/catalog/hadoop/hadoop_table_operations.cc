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

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/util/macros.h"

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

  // (5) MVP: write `vN.metadata.json` without codec. Codec support is added
  // in H15 once the gzip writer is wired in.
  const int64_t next_version = current.version + 1;
  const auto codec = MetadataCompressionCodec::kNone;
  const std::string target = MetadataFilePath(table_dir_, next_version, codec);

  // (6) Fail-if-exists belt: even if the lock manager raced, refuse to clobber
  // an existing v{N} file.
  ICEBERG_ASSIGN_OR_RAISE(auto target_exists, file_io_->Exists(target));
  if (target_exists) {
    return CommitFailed(
        "HadoopTableOperations::Commit: target metadata file '{}' already exists.",
        target);
  }

  // (7) Write the new metadata file.
  ICEBERG_RETURN_UNEXPECTED(TableMetadataUtil::Write(*file_io_, target, updated));

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
  // (10) Lock is released by `guard` at scope exit.
  return {};
}

}  // namespace iceberg::hadoop
