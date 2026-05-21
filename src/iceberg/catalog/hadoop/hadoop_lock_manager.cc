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

#include "iceberg/catalog/hadoop/hadoop_lock_manager.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_map>

namespace iceberg::hadoop {

// ---------------------------------------------------------------------------
// InMemoryLockManager
// ---------------------------------------------------------------------------

struct InMemoryLockManager::Impl {
  struct Entry {
    std::string owner;
    bool held = false;
  };

  std::mutex map_mutex;
  std::condition_variable cv;
  std::unordered_map<std::string, Entry> entries;
};

InMemoryLockManager::InMemoryLockManager(const HadoopCatalogProperties& config)
    : impl_(std::make_unique<Impl>()),
      acquire_timeout_(std::chrono::milliseconds(
          config.Get(HadoopCatalogProperties::kLockAcquireTimeoutMs))),
      acquire_interval_(std::chrono::milliseconds(
          config.Get(HadoopCatalogProperties::kLockAcquireIntervalMs))) {}

InMemoryLockManager::~InMemoryLockManager() = default;

Result<bool> InMemoryLockManager::Acquire(const std::string& entity_id,
                                          const std::string& owner_id) {
  std::unique_lock lock(impl_->map_mutex);
  const auto deadline = std::chrono::steady_clock::now() + acquire_timeout_;

  auto try_take = [&]() -> bool {
    auto& entry = impl_->entries[entity_id];
    if (!entry.held) {
      entry.held = true;
      entry.owner = owner_id;
      return true;
    }
    return false;
  };

  while (true) {
    if (try_take()) {
      return true;
    }
    auto remaining = deadline - std::chrono::steady_clock::now();
    if (remaining <= std::chrono::milliseconds(0)) {
      return false;
    }
    // Wait either for the lock to be released or for the next probe tick,
    // whichever comes first.
    auto wait = std::min<std::chrono::steady_clock::duration>(
        remaining, std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                       acquire_interval_));
    impl_->cv.wait_for(lock, wait);
  }
}

Status InMemoryLockManager::Release(const std::string& entity_id,
                                    const std::string& owner_id) {
  std::lock_guard lock(impl_->map_mutex);
  auto it = impl_->entries.find(entity_id);
  if (it == impl_->entries.end() || !it->second.held) {
    return NotAllowed("InMemoryLockManager: '{}' is not held; cannot release.",
                      entity_id);
  }
  if (it->second.owner != owner_id) {
    return NotAllowed(
        "InMemoryLockManager: '{}' is held by '{}', cannot be released by '{}'.",
        entity_id, it->second.owner, owner_id);
  }
  it->second.held = false;
  it->second.owner.clear();
  impl_->cv.notify_all();
  return {};
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

Result<std::unique_ptr<LockManager>> MakeLockManager(
    const HadoopCatalogProperties& config) {
  const auto impl = config.Get(HadoopCatalogProperties::kLockImpl);
  if (impl.empty() || impl == "in-memory") {
    return std::make_unique<InMemoryLockManager>(config);
  }
  if (impl == "file") {
    return NotSupported(
        "commit.lock-impl=file requires FileLockManager which is introduced in a "
        "subsequent commit (H14).");
  }
  return InvalidArgument("Unknown commit.lock-impl '{}'; expected 'in-memory' or 'file'.",
                         impl);
}

}  // namespace iceberg::hadoop
