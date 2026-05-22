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
/// `FileLockManager` writes a `_lock` file under
/// `<entity_id>/metadata/_lock` using fail-if-exists semantics. The file
/// contents are `<owner_id>|<acquire_ts_ms>` so that another process can
/// detect when the holder died -- the lock is considered stale when its
/// recorded timestamp is older than `lock.heartbeat-timeout-ms`. Stale
/// locks are deleted and re-acquired by the next caller.
///
/// A background heartbeat thread refreshes the lock's timestamp every
/// `lock.heartbeat-interval-ms` so that legitimately slow commits do not
/// race against the stale-lock GC. The thread verifies the lock still
/// belongs to us before refreshing -- if another process stole the lock
/// (we hung past `heartbeat-timeout-ms`), the heartbeat becomes a no-op and
/// Release will surface kNotAllowed.
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
///    compare-and-write primitive available in `FileIO`, so a stealer
///    that deletes our stale lock and re-acquires between our heartbeat
///    read and write will see its lock body overwritten by our stale
///    refresh. Consequences are bounded:
///    - Mutual exclusion of `Acquire` is preserved by the rename CAS at
///      step (1); no third party can take the lock while either of us
///      holds it on disk.
///    - The stealer's subsequent `Release` will fail with
///      `kNotAllowed` because the body says `owner_id` belongs to us.
///    - The lock file may persist (with the wrong recorded owner) until
///      another heartbeat-timeout-ms passes, at which point the
///      stale-lock GC reclaims it.
///    - HadoopCatalog's commit-time CAS on `v{N}.metadata.json`
///      prevents any data corruption that the bounded mis-attribution
///      could otherwise cause.
///
/// In short: the lock body is informational; the real safety is the
/// Acquire-time rename CAS plus the commit-time metadata rename CAS.
class ICEBERG_HADOOP_EXPORT FileLockManager : public LockManager {
 public:
  FileLockManager(std::shared_ptr<FileIO> file_io, const HadoopCatalogProperties& config);
  ~FileLockManager() override;

  Result<bool> Acquire(const std::string& entity_id,
                       const std::string& owner_id) override;
  Status Release(const std::string& entity_id, const std::string& owner_id) override;

 private:
  struct HeartbeatState;
  void RunHeartbeat(HeartbeatState* state);
  void StopHeartbeat(std::unique_ptr<HeartbeatState> state);

  std::shared_ptr<FileIO> file_io_;
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
