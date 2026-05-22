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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <format>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/catalog/hadoop/hadoop_log.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"
#include "iceberg/util/timepoint.h"
#include "iceberg/util/uuid.h"

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
  // Erase the entry rather than mark it free; otherwise a long-running process
  // that touches many distinct tables grows the map unboundedly.
  impl_->entries.erase(it);
  impl_->cv.notify_all();
  return {};
}

// ---------------------------------------------------------------------------
// FileLockManager
// ---------------------------------------------------------------------------

namespace {

int64_t MillisSinceEpoch() { return UnixMsFromTimePointMs(CurrentTimePointMs()); }

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
  ICEBERG_ASSIGN_OR_RAISE(parsed.ts_ms,
                          StringUtils::ParseNumber<int64_t>(body.substr(pipe + 1)));
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
  // Wake any acquirers parked in the Acquire retry loop and then drain held
  // heartbeats. Order matters: signal shutdown first so threads observe it
  // when StopHeartbeat below unlocks them.
  {
    std::lock_guard lk(shutdown_mutex_);
    shutdown_.store(true);
  }
  shutdown_cv_.notify_all();

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

// Lifetime contract: the closure spawned at Acquire captures raw `this` and
// `state` pointer. Both are guaranteed alive for the lifetime of this thread
// because (a) `state` is owned by `states_` until Release moves it out and
// joins the thread, and (b) `~FileLockManager` signals shutdown_ + drains
// `states_` + joins every thread before destroying its fields. So we never
// observe a freed `this` or `state` inside RunHeartbeat.
void FileLockManager::RunHeartbeat(HeartbeatState* state) {
  while (true) {
    std::unique_lock lk(state->wake_mutex);
    state->wake_cv.wait_for(lk, heartbeat_interval_,
                            [state] { return state->stop.load(); });
    if (state->stop.load()) {
      return;
    }
    lk.unlock();

    // Refresh the on-disk timestamp. The read-then-write sequence below
    // is NOT atomic: a stealer that deletes a stale lock and reacquires
    // between our read (which observes our owner_id) and our write will
    // have its lock body silently overwritten by ours. Consequences are
    // documented in the FileLockManager header -- mutual exclusion of
    // Acquire is preserved by the rename CAS, and HadoopCatalog's
    // commit-time CAS prevents any actual data corruption; the lock
    // body is informational.
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
  const std::string lock_dir = MetadataDir(entity_id);
  // CreateDir is idempotent ("mkdir -p" semantics). Failures here mean the
  // FileIO can't even create the metadata directory -- there is no point
  // retrying in the Acquire loop, so propagate.
  if (auto create = file_io_->CreateDir(lock_dir); !create.has_value()) {
    return std::unexpected<Error>(create.error());
  }

  const auto deadline = std::chrono::steady_clock::now() + acquire_timeout_;
  bool stale_warned_this_call = false;
  while (true) {
    // CAS attempt: write the body to a UUID-named temp path, then rename
    // into place with overwrite=false. The rename is the atomic primitive
    // (POSIX link(2) on LocalFileSystem -- see arrow_io.cc::Rename).
    // OutputFile::Create() in arrow does GetFileInfo+OpenOutputStream
    // which is TOCTOU, so we deliberately do NOT use it here.
    const std::string body = EncodeLockBody(owner_id, MillisSinceEpoch());
    const std::string tmp_path =
        std::format("{}/_lock.tmp.{}", lock_dir, Uuid::GenerateV7().ToString());
    auto write_status = file_io_->WriteFile(tmp_path, body);
    if (!write_status.has_value()) {
      // Real infrastructure error -- propagate rather than spin waiting
      // for the lock file to materialise.
      return std::unexpected<Error>(write_status.error());
    }
    {
      auto rename_status = file_io_->Rename(tmp_path, lock_path, /*overwrite=*/false);
      if (rename_status.has_value()) {
        auto state = std::make_unique<HeartbeatState>();
        state->lock_path = lock_path;
        state->owner_id = owner_id;
        HeartbeatState* state_ptr = state.get();
        bool collision = false;
        {
          // Hold states_mutex_ across the emplace AND thread spawn so a
          // racing destruction cannot leave a thread alive without an
          // owning state. We do NOT touch a colliding existing entry --
          // it represents a live previous-Acquire whose heartbeat thread
          // is still running; tearing it down here would require a
          // join-under-mutex (deadlock-prone) AND would clobber its
          // on-disk lock body.
          std::lock_guard lk(states_mutex_);
          auto [it, inserted] = states_.emplace(entity_id, std::move(state));
          if (!inserted) {
            collision = true;
          } else {
            it->second->thread =
                std::thread([this, state_ptr] { RunHeartbeat(state_ptr); });
          }
        }
        if (collision) {
          // The on-disk lock we just published is OUR doing (the rename
          // succeeded means lock_path didn't exist before). The existing
          // states_ entry must therefore be from a Release-that-didn't-
          // clean-up bug; surface that rather than silently smashing it.
          // Delete the just-published on-disk lock so a future Acquire
          // can proceed once the duplicate state is sorted out.
          std::ignore = file_io_->DeleteFile(lock_path);
          return InvalidArgument(
              "FileLockManager: entity '{}' already has an unreleased local "
              "heartbeat state; release it before re-acquiring.",
              entity_id);
        }
        return true;
      }
      // Rename failed. Clean up our temp body first.
      std::ignore = file_io_->DeleteFile(tmp_path);
      // Distinguish "lost the CAS race" (kAlreadyExists -- inspect the
      // existing lock body to decide stale vs busy) from real
      // infrastructure errors (permission denied, backend doesn't support
      // rename, IO error). Real errors must propagate so the caller sees
      // the actual reason instead of a misleading "timeout".
      if (rename_status.error().kind != ErrorKind::kAlreadyExists) {
        return std::unexpected<Error>(rename_status.error());
      }
    }

    // Previous revisions tried to "snatch" stale locks here via
    // Rename(lock_path -> unique stale path, overwrite=false). That is
    // fundamentally unsafe without a "verify body THEN unlink source"
    // primitive: the snatch can atomically move a fresh post-stale lock
    // that a third racer just published, and the best-effort restore
    // that follows is itself racy. We leave the stale file in place.
    // `lock-impl=file` is documented as best-effort; stale-lock
    // reclamation is an operator responsibility (or an external reaper)
    // rather than something inline on the Acquire fast path.
    auto existing = file_io_->ReadFile(lock_path, /*length=*/std::nullopt);
    if (!existing.has_value()) {
      // Disambiguate "file just got deleted by a Release racing us" from a
      // real read error (permission denied, IO failure). The former is
      // expected and we should keep retrying; the latter must propagate
      // so the caller doesn't see a misleading kCommitFailed timeout.
      auto still_there = file_io_->Exists(lock_path);
      if (!still_there.has_value()) {
        return std::unexpected<Error>(still_there.error());
      }
      if (*still_there) {
        // File exists but we couldn't read it -- propagate the original
        // ReadFile error.
        return std::unexpected<Error>(existing.error());
      }
      // Race with Release / external delete: fall through to the wait
      // tick and try Acquire again next iteration.
    } else if (!stale_warned_this_call) {
      if (auto decoded = DecodeLockBody(*existing); decoded.has_value()) {
        const int64_t age_ms = MillisSinceEpoch() - decoded->ts_ms;
        if (age_ms > heartbeat_timeout_.count()) {
          LogWarning(std::format(
              "FileLockManager: '{}' appears stale (owner='{}', age={}ms > "
              "heartbeat-timeout-ms={}ms); waiting for external cleanup. Delete "
              "the file manually if you are sure the holder is gone.",
              lock_path, decoded->owner, age_ms, heartbeat_timeout_.count()));
          stale_warned_this_call = true;
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
    // Park on the shutdown cv so destruction can wake parked acquirers
    // instead of letting them serve the full interval.
    std::unique_lock lk(shutdown_mutex_);
    shutdown_cv_.wait_for(lk, wait, [this] { return shutdown_.load(); });
    if (shutdown_.load()) {
      return false;
    }
  }
}

Status FileLockManager::Release(const std::string& entity_id,
                                const std::string& owner_id) {
  if (file_io_ == nullptr) {
    return InvalidArgument("FileLockManager: FileIO is null.");
  }
  const std::string lock_path = LockFilePath(entity_id);

  // Verify ownership FIRST. A misrouted Release(entity, wrong_owner) used
  // to stop the real holder's heartbeat before returning kNotAllowed --
  // the real holder's still-live lock would then go stale and someone
  // else could steal it. By reading + decoding the body before touching
  // states_ / heartbeat, an unauthorised Release leaves the holder's
  // bookkeeping intact.
  ICEBERG_ASSIGN_OR_RAISE(auto body, file_io_->ReadFile(lock_path, std::nullopt));
  ICEBERG_ASSIGN_OR_RAISE(auto decoded, DecodeLockBody(body));
  if (decoded.owner != owner_id) {
    return NotAllowed("FileLockManager: '{}' is held by '{}', not by '{}'.", lock_path,
                      decoded.owner, owner_id);
  }

  // Ownership verified; now it is safe to stop the heartbeat thread and
  // unlink the on-disk lock. There is a small race between the verify
  // above and the body actually being ours at delete-time, but that
  // window is the same one the heartbeat refresh has -- if a stealer
  // raced in, our DeleteFile removes their body. The Acquire-time rename
  // CAS at the bottom of the publish path is the actual safety net; the
  // lock body is informational.
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
  return file_io_->DeleteFile(lock_path);
}

// ---------------------------------------------------------------------------
// LockReleaseGuard
// ---------------------------------------------------------------------------

LockReleaseGuard::LockReleaseGuard(LockManager* manager, std::string entity_id,
                                   std::string owner_id)
    : manager_(manager),
      entity_id_(std::move(entity_id)),
      owner_id_(std::move(owner_id)),
      active_(manager != nullptr) {}

LockReleaseGuard::~LockReleaseGuard() {
  if (!active_ || manager_ == nullptr) {
    return;
  }
  if (auto status = manager_->Release(entity_id_, owner_id_); !status.has_value()) {
    LogWarning(std::format("HadoopCatalog: lock release failed for '{}' (owner '{}'): {}",
                           entity_id_, owner_id_, status.error().message));
  }
}

void LockReleaseGuard::Dismiss() { active_ = false; }

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

Result<std::unique_ptr<LockManager>> MakeLockManager(
    const HadoopCatalogProperties& config) {
  return MakeLockManagerWithIO(config, /*file_io=*/nullptr);
}

Result<std::unique_ptr<LockManager>> MakeLockManagerWithIO(
    const HadoopCatalogProperties& config, std::shared_ptr<FileIO> file_io) {
  // Validate the config before any LockManager constructor touches Get<int>;
  // ConfigBase::Get<int> would otherwise throw a C++ exception on a
  // malformed value, escaping the Result<> contract. Callers reaching the
  // factory directly (without going through HadoopCatalog::Make) must get
  // the same kInvalidArgument surface treatment.
  ICEBERG_RETURN_UNEXPECTED(config.Validate());

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
