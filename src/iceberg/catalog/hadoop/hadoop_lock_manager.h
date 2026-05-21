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
#include <string>

#include "iceberg/catalog/hadoop/hadoop_catalog_properties.h"
#include "iceberg/catalog/hadoop/iceberg_hadoop_export.h"
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

/// \brief Resolve and construct a `LockManager` from the catalog properties.
///
/// Recognised `lock-impl` values:
/// - "in-memory" (default): `InMemoryLockManager`
/// - "file": `FileLockManager` (introduced in H14)
/// Other values are rejected with `kInvalidArgument` for now; H14 will add a
/// pluggable registry so callers can wire custom implementations.
ICEBERG_HADOOP_EXPORT Result<std::unique_ptr<LockManager>> MakeLockManager(
    const HadoopCatalogProperties& config);

}  // namespace iceberg::hadoop
