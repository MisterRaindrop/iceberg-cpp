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
#include <format>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/util/macros.h"

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
// FileLockManager
// ---------------------------------------------------------------------------

namespace {

int64_t MillisSinceEpoch() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string EncodeLockBody(std::string_view owner_id, int64_t ts_ms) {
  return std::format("{}|{}\n", owner_id, ts_ms);
}

struct LockBody {
  std::string owner;
  int64_t ts_ms = 0;
};
Result<LockBody> DecodeLockBody(std::string_view body) {
  while (!body.empty() && (body.back() == '\n' || body.back() == '\r' ||
                           body.back() == ' ' || body.back() == '\t')) {
    body.remove_suffix(1);
  }
  auto pipe = body.find('|');
  if (pipe == std::string_view::npos || pipe + 1 == body.size()) {
    return Invalid("Malformed lock body: '{}'", body);
  }
  LockBody parsed;
  parsed.owner = std::string(body.substr(0, pipe));
  std::string_view ts_text = body.substr(pipe + 1);
  if (ts_text.empty()) {
    return Invalid("Empty timestamp in lock body");
  }
  int64_t ts = 0;
  for (char c : ts_text) {
    if (c < '0' || c > '9') {
      return Invalid("Non-numeric character '{}' in lock timestamp", c);
    }
    ts = (ts * 10) + (c - '0');
  }
  parsed.ts_ms = ts;
  return parsed;
}

}  // namespace

struct FileLockManager::HeartbeatState {
  std::string lock_path;
  std::string owner_id;
  std::atomic<bool> stop{false};
  std::mutex wake_mutex;
  std::condition_variable wake_cv;
  std::thread thread;
};

FileLockManager::FileLockManager(std::shared_ptr<FileIO> file_io,
                                 const HadoopCatalogProperties& config)
    : file_io_(std::move(file_io)),
      acquire_timeout_(std::chrono::milliseconds(
          config.Get(HadoopCatalogProperties::kLockAcquireTimeoutMs))),
      acquire_interval_(std::chrono::milliseconds(
          config.Get(HadoopCatalogProperties::kLockAcquireIntervalMs))),
      heartbeat_interval_(std::chrono::milliseconds(
          config.Get(HadoopCatalogProperties::kLockHeartbeatIntervalMs))),
      heartbeat_timeout_(std::chrono::milliseconds(
          config.Get(HadoopCatalogProperties::kLockHeartbeatTimeoutMs))) {}

FileLockManager::~FileLockManager() {
  // Stop any heartbeats still running -- typically Release should have
  // drained the map, but defensive cleanup avoids leaking threads if the
  // manager is destroyed while a lock is held.
  std::vector<std::unique_ptr<HeartbeatState>> drain;
  {
    std::lock_guard lk(states_mutex_);
    drain.reserve(states_.size());
    for (auto& [_, state] : states_) {
      drain.push_back(std::move(state));
    }
    states_.clear();
  }
  for (auto& state : drain) {
    StopHeartbeat(std::move(state));
  }
}

void FileLockManager::RunHeartbeat(HeartbeatState* state) {
  while (true) {
    std::unique_lock lk(state->wake_mutex);
    state->wake_cv.wait_for(lk, heartbeat_interval_,
                            [state] { return state->stop.load(); });
    if (state->stop.load()) {
      return;
    }
    lk.unlock();

    // Refresh the on-disk timestamp, but only if we still own the lock.
    // If another process stole it (we hung past heartbeat-timeout-ms), we
    // must not clobber the stealer's lock body.
    auto body = file_io_->ReadFile(state->lock_path, /*length=*/std::nullopt);
    if (!body.has_value()) {
      continue;  // file gone; might be released elsewhere
    }
    auto decoded = DecodeLockBody(*body);
    if (!decoded.has_value() || decoded->owner != state->owner_id) {
      continue;
    }
    std::ignore = file_io_->WriteFile(
        state->lock_path, EncodeLockBody(state->owner_id, MillisSinceEpoch()));
  }
}

void FileLockManager::StopHeartbeat(std::unique_ptr<HeartbeatState> state) {
  {
    std::lock_guard lk(state->wake_mutex);
    state->stop.store(true);
  }
  state->wake_cv.notify_all();
  if (state->thread.joinable()) {
    state->thread.join();
  }
}

Result<bool> FileLockManager::Acquire(const std::string& entity_id,
                                      const std::string& owner_id) {
  if (file_io_ == nullptr) {
    return InvalidArgument("FileLockManager: FileIO is null.");
  }
  const std::string lock_path = LockFilePath(entity_id);
  std::ignore = file_io_->CreateDir(MetadataDir(entity_id));

  const auto deadline = std::chrono::steady_clock::now() + acquire_timeout_;
  while (true) {
    auto out_file = file_io_->NewOutputFile(lock_path);
    if (out_file.has_value()) {
      auto stream = (*out_file)->Create();
      if (stream.has_value()) {
        const std::string body = EncodeLockBody(owner_id, MillisSinceEpoch());
        auto bytes = std::as_bytes(std::span(body.data(), body.size()));
        auto write = (*stream)->Write(bytes);
        auto flush = write.has_value() ? (*stream)->Flush() : Status{};
        auto close_status = (*stream)->Close();
        if (write.has_value() && flush.has_value() && close_status.has_value()) {
          // Spawn the heartbeat thread so a legitimately slow commit does
          // not get its lock stolen by the stale-lock GC.
          auto state = std::make_unique<HeartbeatState>();
          state->lock_path = lock_path;
          state->owner_id = owner_id;
          HeartbeatState* state_ptr = state.get();
          state->thread = std::thread([this, state_ptr] { RunHeartbeat(state_ptr); });
          {
            std::lock_guard lk(states_mutex_);
            states_.emplace(entity_id, std::move(state));
          }
          return true;
        }
        std::ignore = file_io_->DeleteFile(lock_path);
      }
    }

    auto existing = file_io_->ReadFile(lock_path, /*length=*/std::nullopt);
    if (existing.has_value()) {
      auto decoded = DecodeLockBody(*existing);
      if (decoded.has_value()) {
        const int64_t age_ms = MillisSinceEpoch() - decoded->ts_ms;
        if (age_ms > heartbeat_timeout_.count()) {
          std::ignore = file_io_->DeleteFile(lock_path);
          continue;
        }
      }
    }
    auto remaining = deadline - std::chrono::steady_clock::now();
    if (remaining <= std::chrono::milliseconds(0)) {
      return false;
    }
    auto wait = std::min<std::chrono::steady_clock::duration>(
        remaining, std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                       acquire_interval_));
    std::this_thread::sleep_for(wait);
  }
}

Status FileLockManager::Release(const std::string& entity_id,
                                const std::string& owner_id) {
  if (file_io_ == nullptr) {
    return InvalidArgument("FileLockManager: FileIO is null.");
  }
  const std::string lock_path = LockFilePath(entity_id);

  // Stop the heartbeat thread before reading the on-disk body so the
  // background refresh cannot race with our delete.
  std::unique_ptr<HeartbeatState> state;
  {
    std::lock_guard lk(states_mutex_);
    auto it = states_.find(entity_id);
    if (it != states_.end()) {
      state = std::move(it->second);
      states_.erase(it);
    }
  }
  if (state != nullptr) {
    StopHeartbeat(std::move(state));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto body, file_io_->ReadFile(lock_path, std::nullopt));
  ICEBERG_ASSIGN_OR_RAISE(auto decoded, DecodeLockBody(body));
  if (decoded.owner != owner_id) {
    return NotAllowed("FileLockManager: '{}' is held by '{}', not by '{}'.", lock_path,
                      decoded.owner, owner_id);
  }
  return file_io_->DeleteFile(lock_path);
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

Result<std::unique_ptr<LockManager>> MakeLockManager(
    const HadoopCatalogProperties& config) {
  return MakeLockManagerWithIO(config, /*file_io=*/nullptr);
}

Result<std::unique_ptr<LockManager>> MakeLockManagerWithIO(
    const HadoopCatalogProperties& config, std::shared_ptr<FileIO> file_io) {
  const auto impl = config.Get(HadoopCatalogProperties::kLockImpl);
  if (impl.empty() || impl == "in-memory") {
    return std::make_unique<InMemoryLockManager>(config);
  }
  if (impl == "file") {
    if (file_io == nullptr) {
      return InvalidArgument(
          "commit.lock-impl=file requires a FileIO; use MakeLockManagerWithIO from "
          "HadoopCatalog::Make.");
    }
    return std::make_unique<FileLockManager>(std::move(file_io), config);
  }
  return InvalidArgument("Unknown commit.lock-impl '{}'; expected 'in-memory' or 'file'.",
                         impl);
}

}  // namespace iceberg::hadoop
