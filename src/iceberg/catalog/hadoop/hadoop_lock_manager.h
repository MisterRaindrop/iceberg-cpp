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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "iceberg/catalog/hadoop/hadoop_catalog_properties.h"
#include "iceberg/catalog/hadoop/iceberg_hadoop_export.h"
#include "iceberg/file_io.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/hadoop/hadoop_lock_manager.h
/// \brief LockManager abstraction used by HadoopTableOperations to serialise
/// commits to the same table.

namespace iceberg::hadoop {

/// \brief Mutual exclusion primitive for HadoopCatalog commits.
///
/// HadoopCatalog uses a LockManager to make commit-time CAS racy-but-correct:
/// the real CAS primitive is the atomic
/// `Rename(temp, v{N+1}.metadata.json, overwrite=false)` performed during
/// commit -- only one writer can publish v{N+1}. The LockManager serialises
/// writers around that rename so the CAS retry rate stays low when many
/// commits race for the same table; `version-hint.text` is a separate
/// atomic-replace (`Rename(overwrite=true)`) published AFTER the metadata
/// rename wins, never the source of truth on its own. The LockManager
/// mirrors Java's `org.apache.iceberg.util.LockManager` interface so
/// configuration is portable across language implementations.
class ICEBERG_HADOOP_EXPORT LockManager {
 public:
  virtual ~LockManager() = default;

  /// \brief Acquire the lock for `entity_id` on behalf of `owner_id`. Returns
  /// true when the lock is acquired, false when the acquire timeout elapses,
  /// or a non-OK Result on infrastructure failure (e.g. a Filesystem error
  /// in `FileLockManager`). Implementations should retry internally until the
  /// timeout is exhausted.
  virtual Result<bool> Acquire(const std::string& entity_id,
                               const std::string& owner_id) = 0;

  /// \brief Release a previously acquired lock. Releasing a lock not held by
  /// the caller is implementation-defined; the default semantics return
  /// `kNotAllowed`.
  virtual Status Release(const std::string& entity_id, const std::string& owner_id) = 0;
};

/// \brief In-memory, per-process implementation of `LockManager`.
///
/// This is the default lock manager. It is a strict mutex map keyed by entity
/// id: callers from different threads serialize, callers from different
/// processes are not coordinated. The heartbeat-related properties on
/// `HadoopCatalogProperties` are accepted for configuration compatibility
/// with Java but are no-ops here (a held `std::mutex` cannot expire). For
/// true cross-process serialization, see `FileLockManager`.
class ICEBERG_HADOOP_EXPORT InMemoryLockManager : public LockManager {
 public:
  /// \brief Construct using the lock-related properties from
  /// `HadoopCatalogProperties` (specifically `lock.acquire-interval-ms` and
  /// `lock.acquire-timeout-ms`).
  explicit InMemoryLockManager(const HadoopCatalogProperties& config);
  ~InMemoryLockManager() override;

  Result<bool> Acquire(const std::string& entity_id,
                       const std::string& owner_id) override;
  Status Release(const std::string& entity_id, const std::string& owner_id) override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  std::chrono::milliseconds acquire_timeout_;
  std::chrono::milliseconds acquire_interval_;
};

/// \brief Filesystem-backed lock manager.
///
/// `FileLockManager` writes one lock file per entity under a single
/// warehouse-shared directory:
/// `<warehouse>/_iceberg_catalog_locks/<FNV1a64-hex(canonical(entity_id))>.lock`.
/// Files are created via UUID-named temp + `Rename(overwrite=false)` so
/// the rename CAS is fail-if-exists. Storing locks OUTSIDE each table's
/// own `metadata/` directory means `DropTable(purge=true)` (which
/// recursively deletes the table tree) cannot destroy an in-flight
/// lock file. The file contents are `<owner_id>|<acquire_ts_ms>`; the
/// recorded timestamp lets a separate reaper / operator decide when the
/// lock is no longer live. A background heartbeat thread refreshes the
/// timestamp every `lock.heartbeat-interval-ms` so a legitimately slow
/// commit is not mis-classified as a dead holder.
///
/// **Stale-lock reclamation is NOT performed inline by `Acquire`.** A
/// `read body -> rename source aside` snatch is fundamentally unsafe
/// without a "verify body THEN unlink source" primitive (it can move a
/// fresh post-stale lock that a third racer just published). `Acquire`
/// therefore leaves any stale `_lock` file in place and logs a warning
/// once per call. Operators (or an external reaper that knows the
/// expected holder is gone) must remove the file; after removal the next
/// `Acquire` succeeds normally via the rename CAS.
///
/// **Cross-process atomicity envelope.**
///
/// 1. **Acquire is a true CAS on LocalFileSystem only.** iceberg-cpp's
///    `Rename(overwrite=false)` is atomic create-if-absent via
///    `std::filesystem::create_hard_link` (POSIX `link(2)` returns
///    EEXIST). Two competing acquirers cannot both publish `_lock`; the
///    loser observes `kAlreadyExists`. On every other arrow-backed
///    FileIO (HDFS `hdfs://`, S3 `s3://`/`s3a://`/`s3n://`, `mockfs://`)
///    arrow's `Move` does not expose a portable create-if-absent
///    primitive, so the implementation falls back to a `GetFileInfo`
///    precheck + unconditional `Move`. That fallback is TOCTOU.
///    Production deployments that need cross-process serialisation on
///    HDFS/S3 must layer their own coordination (DynamoDB lock manager,
///    S3 conditional PUT, etc.).
///
/// 2. **Heartbeat refresh is best-effort even on LocalFileSystem.** The
///    background heartbeat thread reads the current body, checks owner
///    equality, then writes the new timestamp. There is no atomic
///    compare-and-write primitive available in `FileIO`, so if the
///    on-disk lock is externally reaped and another acquirer publishes a
///    fresh body between our heartbeat read and write, our stale write
///    can clobber the new body. The race window is bounded (microseconds),
///    consequences are bounded too: the stealer's later `Release` returns
///    `kNotAllowed`, HadoopCatalog's commit-time CAS on
///    `v{N+1}.metadata.json` prevents any actual data corruption, and
///    the lock body is informational rather than the source of truth.
///
/// In short: the lock body is informational; the real safety is the
/// Acquire-time rename CAS plus the commit-time metadata rename CAS.
class ICEBERG_HADOOP_EXPORT FileLockManager : public LockManager {
 public:
  /// \param file_io  FileIO backing the warehouse.
  /// \param config   Catalog config (timeouts).
  /// \param lock_root Directory where _lock files live. Lock files are
  ///   placed at `<lock_root>/<encoded(entity_id)>.lock` -- crucially
  ///   OUTSIDE the table directory the lock guards, so DropTable can
  ///   recursively delete the table dir without removing the lock file
  ///   (and without racing peers between Release and rmdir).
  FileLockManager(std::shared_ptr<FileIO> file_io, const HadoopCatalogProperties& config,
                  std::string lock_root);
  ~FileLockManager() override;

  Result<bool> Acquire(const std::string& entity_id,
                       const std::string& owner_id) override;
  Status Release(const std::string& entity_id, const std::string& owner_id) override;

  /// \brief Return the on-disk lock file path for `entity_id`. Public so
  /// tests (and HadoopCatalog DropTable) can introspect without
  /// duplicating the encoder.
  std::string LockFilePathFor(std::string_view entity_id) const;

 private:
  struct HeartbeatState;
  void RunHeartbeat(HeartbeatState* state);
  void StopHeartbeat(std::unique_ptr<HeartbeatState> state);

  std::shared_ptr<FileIO> file_io_;
  std::string lock_root_;
  std::chrono::milliseconds acquire_timeout_;
  std::chrono::milliseconds acquire_interval_;
  std::chrono::milliseconds heartbeat_interval_;
  std::chrono::milliseconds heartbeat_timeout_;

  std::mutex states_mutex_;
  std::unordered_map<std::string, std::unique_ptr<HeartbeatState>> states_;

  // Wakes acquirers parked on the retry interval during ~FileLockManager so
  // destruction does not block on a full acquire-interval-ms tick.
  std::mutex shutdown_mutex_;
  std::condition_variable shutdown_cv_;
  std::atomic<bool> shutdown_{false};
};

/// \brief RAII guard that releases a held lock at scope exit.
///
/// Holders set `active = true` after a successful `Acquire`; the destructor
/// releases the lock and routes any release failure through `LogWarning` so
/// operators see lock leaks instead of silent swallowing. Used by both
/// `HadoopCatalog::CreateTable` and `HadoopTableOperations::Commit` to keep
/// their error paths linear.
class ICEBERG_HADOOP_EXPORT LockReleaseGuard {
 public:
  LockReleaseGuard(LockManager* manager, std::string entity_id, std::string owner_id);
  ~LockReleaseGuard();
  LockReleaseGuard(const LockReleaseGuard&) = delete;
  LockReleaseGuard& operator=(const LockReleaseGuard&) = delete;
  LockReleaseGuard(LockReleaseGuard&&) = delete;
  LockReleaseGuard& operator=(LockReleaseGuard&&) = delete;

  /// \brief Disarm the guard so the lock is not released at scope exit. Used
  /// when ownership of the held lock has been transferred elsewhere.
  void Dismiss();

 private:
  LockManager* manager_;
  std::string entity_id_;
  std::string owner_id_;
  bool active_;
};

/// \brief Resolve and construct a `LockManager` from the catalog properties.
///
/// Recognised `lock-impl` values:
/// - "in-memory" (default): `InMemoryLockManager`
/// - "file": `FileLockManager` (requires a FileIO -- pass it explicitly
///   via `MakeLockManagerWithIO`).
ICEBERG_HADOOP_EXPORT Result<std::unique_ptr<LockManager>> MakeLockManager(
    const HadoopCatalogProperties& config);

/// \brief Variant of `MakeLockManager` that can return a FileLockManager.
/// HadoopCatalog passes its FileIO through this entry point.
ICEBERG_HADOOP_EXPORT Result<std::unique_ptr<LockManager>> MakeLockManagerWithIO(
    const HadoopCatalogProperties& config, std::shared_ptr<FileIO> file_io);

}  // namespace iceberg::hadoop
