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

#include <chrono>
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
/// the filesystem rename of `version-hint.text` is the source of truth, but
/// holding a lock around the rename window reduces the rate of CAS retries
/// when many writers race for the same table. The LockManager mirrors Java's
/// `org.apache.iceberg.util.LockManager` interface so configuration is
/// portable across language implementations.
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
/// true cross-process serialization, see `FileLockManager` in H14.
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
