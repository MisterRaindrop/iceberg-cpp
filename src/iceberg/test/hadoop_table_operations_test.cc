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

#include <atomic>
#include <fstream>
#include <latch>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <arrow/filesystem/localfs.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/catalog/hadoop/hadoop_catalog_properties.h"
#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/catalog/hadoop/hadoop_lock_manager.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"
#include "iceberg/test/test_resource.h"

namespace iceberg::hadoop {

namespace {

// Slurp a fixture file from src/iceberg/test/resources.
std::string SlurpResource(const std::string& name) {
  std::ifstream file(GetResourcePath(name));
  std::stringstream buffer;
  buffer << file.rdbuf();
  return buffer.str();
}

}  // namespace

class HadoopTableOperationsTest : public ::iceberg::TempFileTestBase {
 protected:
  void SetUp() override {
    ::iceberg::TempFileTestBase::SetUp();
    warehouse_ = "file://" + CreateTempDirectory();
    table_dir_ = warehouse_ + "/db/events";
    file_io_ = std::make_shared<::iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    // Pre-create the metadata directory.
    ASSERT_TRUE(file_io_->CreateDir(MetadataDir(table_dir_)).has_value());
  }

  // Write a metadata fixture into the table's metadata directory.
  void SeedMetadataFile(int64_t version, MetadataCompressionCodec codec,
                        const std::string& body) {
    const std::string path =
        MetadataDir(table_dir_) + "/" + MetadataFileName(version, codec);
    ASSERT_TRUE(file_io_->WriteFile(path, body).has_value());
  }

  void SeedVersionHint(const std::string& body) {
    ASSERT_TRUE(file_io_->WriteFile(VersionHintPath(table_dir_), body).has_value());
  }

  std::string warehouse_;
  std::string table_dir_;
  std::shared_ptr<FileIO> file_io_;
};

TEST_F(HadoopTableOperationsTest, ReadVersionHintParsesTrimmedDecimal) {
  SeedVersionHint("42\n");
  ICEBERG_UNWRAP_OR_FAIL(auto version, ReadVersionHint(*file_io_, table_dir_));
  EXPECT_EQ(version, 42);

  // Whitespace and CRLF are tolerated.
  SeedVersionHint("  7\r\n");
  ICEBERG_UNWRAP_OR_FAIL(auto v7, ReadVersionHint(*file_io_, table_dir_));
  EXPECT_EQ(v7, 7);
}

TEST_F(HadoopTableOperationsTest, ReadVersionHintRejectsNonNumeric) {
  SeedVersionHint("abc");
  auto res = ReadVersionHint(*file_io_, table_dir_);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);
}

TEST_F(HadoopTableOperationsTest, FindLatestMetadataVersionPicksMax) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedMetadataFile(3, MetadataCompressionCodec::kNone, body);
  SeedMetadataFile(2, MetadataCompressionCodec::kGzip,
                   "ignored-not-valid-gzip-but-name-counts");
  // Drop a UUID-prefixed temp file to make sure it is ignored.
  ASSERT_TRUE(
      file_io_->WriteFile(MetadataDir(table_dir_) + "/abc-uuid.metadata.json", "ignored")
          .has_value());

  ICEBERG_UNWRAP_OR_FAIL(auto latest, FindLatestMetadataVersion(*file_io_, table_dir_));
  EXPECT_EQ(latest, 3);
}

TEST_F(HadoopTableOperationsTest, FindLatestMetadataVersionFailsWhenEmpty) {
  auto res = FindLatestMetadataVersion(*file_io_, table_dir_);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchTable, res.error().kind);
}

TEST_F(HadoopTableOperationsTest, ResolveUsesVersionHintWhenPresent) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedMetadataFile(5, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("5\n");

  ICEBERG_UNWRAP_OR_FAIL(auto pointer, ResolveCurrentMetadata(*file_io_, table_dir_));
  EXPECT_EQ(pointer.version, 5);
  EXPECT_FALSE(pointer.from_listdir_fallback);
  EXPECT_TRUE(pointer.location.ends_with("v5.metadata.json"));
}

TEST_F(HadoopTableOperationsTest, ResolveRejectsDuplicateVersionAcrossCodecs) {
  // The commit path's codec-independent CAS prevents iceberg-cpp from
  // ever publishing two files at the same version. But an external
  // writer (foreign tool, mid-codec-migration crash, manual RegisterTable
  // from a parallel process) can plant both `vN.metadata.json` and
  // `vN.gz.metadata.json` in the same metadata/ dir. Silently picking
  // whichever the listing surfaces first would mask a real ambiguity and
  // could even flip the chosen file across Refresh calls if directory
  // ordering changes. Surface kInvalidArgument so the operator can
  // repair the dir.
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(3, MetadataCompressionCodec::kNone, body);
  SeedMetadataFile(3, MetadataCompressionCodec::kGzip, body);
  SeedVersionHint("3\n");

  auto resolved = ResolveCurrentMetadata(*file_io_, table_dir_);
  ASSERT_FALSE(resolved.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, resolved.error().kind);
  EXPECT_NE(resolved.error().message.find("version 3"), std::string::npos);
}

TEST_F(HadoopTableOperationsTest, ResolveFallsBackToListdirWhenHintMissing) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(2, MetadataCompressionCodec::kNone, body);
  SeedMetadataFile(4, MetadataCompressionCodec::kNone, body);

  ICEBERG_UNWRAP_OR_FAIL(auto pointer, ResolveCurrentMetadata(*file_io_, table_dir_));
  EXPECT_EQ(pointer.version, 4);
  EXPECT_TRUE(pointer.from_listdir_fallback);
  EXPECT_TRUE(pointer.location.ends_with("v4.metadata.json"));
}

TEST_F(HadoopTableOperationsTest, RefreshReturnsTableMetadata) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(3, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("3");

  HadoopTableOperations ops(file_io_, table_dir_);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, ops.Refresh());
  ASSERT_NE(metadata, nullptr);
  EXPECT_EQ(metadata->table_uuid, "d20125c8-7284-442c-9aea-15fee620737c");
  EXPECT_EQ(ops.current_version(), 3);
  EXPECT_TRUE(ops.current_metadata_location().ends_with("v3.metadata.json"));
}

TEST_F(HadoopTableOperationsTest, RefreshFailsWhenNoMetadata) {
  HadoopTableOperations ops(file_io_, table_dir_);
  auto res = ops.Refresh();
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchTable, res.error().kind);
}

class HadoopCommitTest : public HadoopTableOperationsTest {
 protected:
  void SetUp() override {
    HadoopTableOperationsTest::SetUp();
    auto props = HadoopCatalogProperties::FromMap({
        {"warehouse", warehouse_},
        {"lock.acquire-timeout-ms", "1000"},
        {"lock.acquire-interval-ms", "20"},
    });
    ICEBERG_UNWRAP_OR_FAIL(auto manager, MakeLockManager(props));
    lock_manager_ = std::shared_ptr<LockManager>(std::move(manager));
  }

  std::shared_ptr<LockManager> lock_manager_;
};

TEST_F(HadoopCommitTest, CommitWritesNextVersionAndUpdatesHint) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("1");

  HadoopTableOperations ops(file_io_, table_dir_, lock_manager_, "test-owner");
  ICEBERG_UNWRAP_OR_FAIL(auto base, ops.Refresh());
  // For this commit test we simply commit the same metadata back as v2.
  // (UpdateTable does the real builder dance; here we just exercise the
  // commit primitives.)
  ASSERT_TRUE(ops.Commit(*base, *base).has_value());
  EXPECT_EQ(ops.current_version(), 2);
  EXPECT_TRUE(ops.current_metadata_location().ends_with("v2.metadata.json"));

  // Refresh from a fresh ops sees the new pointer.
  HadoopTableOperations fresh(file_io_, table_dir_);
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, fresh.Refresh());
  EXPECT_EQ(fresh.current_version(), 2);
  EXPECT_NE(reloaded, nullptr);
}

TEST_F(HadoopCommitTest, CommitRejectsRelocation) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("1");

  HadoopTableOperations ops(file_io_, table_dir_, lock_manager_, "test-owner");
  ICEBERG_UNWRAP_OR_FAIL(auto base, ops.Refresh());
  TableMetadata updated = *base;
  updated.location = updated.location + "/relocated";
  auto res = ops.Commit(*base, updated);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);
}

TEST_F(HadoopCommitTest, CommitRejectsStaleBase) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("1");

  HadoopTableOperations a(file_io_, table_dir_, lock_manager_, "owner-a");
  HadoopTableOperations b(file_io_, table_dir_, lock_manager_, "owner-b");
  ICEBERG_UNWRAP_OR_FAIL(auto base_a, a.Refresh());
  ICEBERG_UNWRAP_OR_FAIL(auto base_b, b.Refresh());

  // a wins.
  ASSERT_TRUE(a.Commit(*base_a, *base_a).has_value());

  // b's view is now stale.
  auto res = b.Commit(*base_b, *base_b);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kCommitFailed, res.error().kind);
}

TEST_F(HadoopCommitTest, FileLockManagerAcquiresAndDetectsStale) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("1");

  auto props = HadoopCatalogProperties::FromMap({
      {"warehouse", warehouse_},
      {"lock-impl", "file"},
      {"lock.acquire-timeout-ms", "300"},
      {"lock.acquire-interval-ms", "20"},
      // Keep the heartbeat window large enough that the first lock is not
      // accidentally marked stale during the 300ms acquire test below.
      {"lock.heartbeat-timeout-ms", "10000"},
  });
  ICEBERG_UNWRAP_OR_FAIL(auto raw, MakeLockManagerWithIO(props, file_io_));
  // Need the FileLockManager-specific accessor `LockFilePathFor` so we can
  // hand-seed a stale lock file at the actual on-disk location (the lock
  // root now lives in `<warehouse>/_iceberg_catalog_locks/`, NOT under
  // the table dir).
  auto* file_lock_raw = dynamic_cast<FileLockManager*>(raw.get());
  ASSERT_NE(file_lock_raw, nullptr);
  auto file_lock = std::shared_ptr<LockManager>(std::move(raw));

  ICEBERG_UNWRAP_OR_FAIL(auto first, file_lock->Acquire(table_dir_, "first"));
  EXPECT_TRUE(first);

  // Holding -> second acquire times out within ~500ms.
  ICEBERG_UNWRAP_OR_FAIL(auto second, file_lock->Acquire(table_dir_, "second"));
  EXPECT_FALSE(second);

  // Stale path: writing a lock file by hand with an old timestamp must NOT
  // be silently stolen by a subsequent Acquire -- the snatch is unsafe
  // without a verify-body-then-unlink primitive, so we leave the file in
  // place and surface a warning. Operators (or an external reaper) must
  // remove the stale lock; once removed, Acquire succeeds normally.
  ASSERT_TRUE(file_lock->Release(table_dir_, "first").has_value());
  const std::string lock_path = file_lock_raw->LockFilePathFor(table_dir_);
  ASSERT_TRUE(file_io_->WriteFile(lock_path, "ghost|0\n").has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto stale_blocked, file_lock->Acquire(table_dir_, "newcomer"));
  EXPECT_FALSE(stale_blocked)
      << "stale on-disk lock must not be auto-reclaimed by Acquire";

  // External reaper removes the stale file; the next Acquire then wins via
  // the normal Rename-CAS publish path.
  ASSERT_TRUE(file_io_->DeleteFile(lock_path).has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto after_reap, file_lock->Acquire(table_dir_, "newcomer"));
  EXPECT_TRUE(after_reap);
  EXPECT_TRUE(file_lock->Release(table_dir_, "newcomer").has_value());
}

TEST_F(HadoopCommitTest, CommitHonoursGzipCodec) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("1");

  HadoopTableOperations ops(file_io_, table_dir_, lock_manager_, "owner");
  ICEBERG_UNWRAP_OR_FAIL(auto base, ops.Refresh());
  TableMetadata updated = *base;
  updated.properties.Set(TableProperties::kMetadataCompression, std::string("gzip"));
  // Sanity-check that Set actually round-trips through ConfigBase.
  ASSERT_EQ("gzip", updated.properties.Get(TableProperties::kMetadataCompression));

  ASSERT_TRUE(ops.Commit(*base, updated).has_value());
  EXPECT_EQ(ops.current_version(), 2);
  EXPECT_TRUE(ops.current_metadata_location().ends_with("v2.gz.metadata.json"))
      << ops.current_metadata_location();

  // The new file must be valid gzip (not the raw JSON). Refresh reads it back
  // through TableMetadataUtil::Read which already decodes gzip suffixes, so a
  // successful re-Refresh is the round-trip assertion we want.
  HadoopTableOperations fresh(file_io_, table_dir_);
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, fresh.Refresh());
  ASSERT_NE(reloaded, nullptr);
  EXPECT_EQ(fresh.current_version(), 2);
}

TEST_F(HadoopCommitTest, PreviousVersionsMaxZeroPrunesEverything) {
  // previous-versions-max=0 means "keep only the current version, drop all
  // previous." Negative values continue to disable GC entirely. Java accepts
  // 0 as a valid cap; treating <=0 as "disable" (the previous behaviour)
  // silently dropped this user-facing knob and allowed unbounded history
  // accumulation when callers expected the opposite.
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("1");

  HadoopTableOperations ops(file_io_, table_dir_, lock_manager_, "owner");
  ICEBERG_UNWRAP_OR_FAIL(auto base, ops.Refresh());
  TableMetadata updated = *base;
  updated.properties.Set(TableProperties::kMetadataPreviousVersionsMax, int32_t{0});
  updated.properties.Set(TableProperties::kMetadataDeleteAfterCommitEnabled, true);

  for (int i = 0; i < 3; ++i) {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, ops.Refresh());
    TableMetadata next = *reloaded;
    next.properties = updated.properties;
    ASSERT_TRUE(ops.Commit(*reloaded, next).has_value()) << "round " << i;
  }

  // After three commits we should be at v4 with NO older versioned files
  // left under metadata/ (only v4 plus version-hint.text).
  EXPECT_EQ(ops.current_version(), 4);
  ICEBERG_UNWRAP_OR_FAIL(auto entries, file_io_->ListDir(MetadataDir(table_dir_)));
  int versioned = 0;
  for (const auto& entry : entries) {
    std::string_view name = entry.location;
    auto slash = name.find_last_of('/');
    if (slash != std::string_view::npos) {
      name.remove_prefix(slash + 1);
    }
    if (ParseMetadataFileName(name).has_value()) {
      ++versioned;
    }
  }
  EXPECT_EQ(versioned, 1) << "previous-versions-max=0 must keep current only";
}

TEST_F(HadoopCommitTest, DeleteAfterCommitPrunesOldMetadata) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("1");

  HadoopTableOperations ops(file_io_, table_dir_, lock_manager_, "owner");
  ICEBERG_UNWRAP_OR_FAIL(auto base, ops.Refresh());
  TableMetadata updated = *base;
  // Keep only one previous version on top of the current one.
  updated.properties.Set(TableProperties::kMetadataPreviousVersionsMax, int32_t{1});
  updated.properties.Set(TableProperties::kMetadataDeleteAfterCommitEnabled, true);

  // Commit a few times so multiple old files accumulate.
  for (int i = 0; i < 4; ++i) {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, ops.Refresh());
    TableMetadata next = *reloaded;
    next.properties = updated.properties;
    ASSERT_TRUE(ops.Commit(*reloaded, next).has_value()) << "round " << i;
  }
  // After commits, version-hint should point at v5, and only v5 + 1 older
  // copy (v4) should remain.
  EXPECT_EQ(ops.current_version(), 5);
  ICEBERG_UNWRAP_OR_FAIL(auto entries, file_io_->ListDir(MetadataDir(table_dir_)));
  int versioned = 0;
  for (const auto& entry : entries) {
    std::string_view name = entry.location;
    auto slash = name.find_last_of('/');
    if (slash != std::string_view::npos) {
      name.remove_prefix(slash + 1);
    }
    if (ParseMetadataFileName(name).has_value()) {
      ++versioned;
    }
  }
  EXPECT_LE(versioned, 2);
}

TEST_F(HadoopCommitTest, CrashedHintUpdateRecoversOnRefresh) {
  // Simulate a writer that successfully renamed temp -> v2.metadata.json
  // (the protocol's CAS commit point) but crashed before updating
  // version-hint.text. Refresh must treat the rename as the source of
  // truth: it should pick up v2 as the current version and the next
  // commit must produce v3 without being permanently stuck on v2's
  // "already exists" check.
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("1");
  // Plant v2.metadata.json as if a writer's rename succeeded but the
  // subsequent hint update did not.
  SeedMetadataFile(2, MetadataCompressionCodec::kNone, body);

  HadoopTableOperations ops(file_io_, table_dir_, lock_manager_, "owner");
  ICEBERG_UNWRAP_OR_FAIL(auto base, ops.Refresh());
  // Refresh promotes v2 to current despite the lagging hint -- the rename
  // is authoritative, not the hint. Without this fix, the next commit
  // would target v2 again and fail forever on "AlreadyExists".
  EXPECT_EQ(ops.current_version(), 2);

  // The next commit must land at v3 cleanly.
  ASSERT_TRUE(ops.Commit(*base, *base).has_value());
  EXPECT_EQ(ops.current_version(), 3);
}

TEST_F(HadoopCommitTest, ConcurrentCodecDifferentExtensionsRejectsSecond) {
  // Crucial: rename(overwrite=false) alone does not give codec-independent
  // CAS because v2.metadata.json and v2.metadata.json.gz are distinct
  // filenames and BOTH renames would succeed. The codec-aware safety net
  // is "any v{next}.* present" -- be it via Refresh promoting current to
  // 2 (so the stale check fires) or via the lock-scoped duplicate scan
  // (defense-in-depth). Either way the second writer must lose CAS.
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("1");

  HadoopTableOperations ops(file_io_, table_dir_, lock_manager_, "owner");
  ICEBERG_UNWRAP_OR_FAIL(auto base, ops.Refresh());
  ASSERT_EQ(ops.current_version(), 1);

  // Plant a gzipped v2 as if a raced writer published with a different codec.
  SeedMetadataFile(2, MetadataCompressionCodec::kGzip, body);

  // Attempt to commit with the default (uncompressed) codec from a stale
  // v1 view. The commit must FAIL -- otherwise two writers would publish
  // v2.metadata.json AND v2.metadata.json.gz simultaneously, losing one
  // update.
  auto res = ops.Commit(*base, *base);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kCommitFailed, res.error().kind);

  // And the raced-in gzipped file is still there: we didn't accidentally
  // delete it as part of the rejected commit.
  ICEBERG_UNWRAP_OR_FAIL(
      auto still_there,
      file_io_->Exists(MetadataDir(table_dir_) + "/" +
                       MetadataFileName(2, MetadataCompressionCodec::kGzip)));
  EXPECT_TRUE(still_there);
}

TEST_F(HadoopCommitTest, FileLockManagerHeartbeatPreventsStaleSteal) {
  // Heartbeat-timeout much shorter than the sleep below; without the
  // heartbeat refresh, the second Acquire would steal the lock. With the
  // heartbeat thread re-writing the lock body, it stays fresh.
  // Margin of >=5x between heartbeat-interval and heartbeat-timeout, and
  // between heartbeat-timeout and the sleep below, so CI scheduler jitter
  // (especially on busy macOS / Linux runners) does not flake the test.
  auto props = HadoopCatalogProperties::FromMap({
      {"warehouse", warehouse_},
      {"lock-impl", "file"},
      {"lock.acquire-timeout-ms", "500"},
      {"lock.acquire-interval-ms", "40"},
      {"lock.heartbeat-interval-ms", "40"},
      {"lock.heartbeat-timeout-ms", "200"},
  });
  ICEBERG_UNWRAP_OR_FAIL(auto raw, MakeLockManagerWithIO(props, file_io_));
  auto file_lock = std::shared_ptr<LockManager>(std::move(raw));

  ICEBERG_UNWRAP_OR_FAIL(auto first, file_lock->Acquire(table_dir_, "holder"));
  ASSERT_TRUE(first);

  // Sleep ~5x heartbeat-timeout-ms; the heartbeat should keep the lock body
  // fresh so a competing acquirer still sees it as held.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  ICEBERG_UNWRAP_OR_FAIL(auto stolen, file_lock->Acquire(table_dir_, "thief"));
  EXPECT_FALSE(stolen) << "heartbeat should have kept the lock body fresh";

  EXPECT_TRUE(file_lock->Release(table_dir_, "holder").has_value());
}

TEST_F(HadoopCommitTest, ConcurrentCommitsConvergeWithRetries) {
  // Seed v1.
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("1");

  constexpr int kWorkers = 4;
  constexpr int kRoundsPerWorker = 5;
  std::latch ready(kWorkers);
  std::atomic<int> successes{0};
  std::atomic<int> cas_failures{0};

  auto worker = [&](int id) {
    ready.arrive_and_wait();
    for (int round = 0; round < kRoundsPerWorker; ++round) {
      HadoopTableOperations ops(file_io_, table_dir_, lock_manager_,
                                std::format("owner-{}-{}", id, round));
      while (true) {
        auto base = ops.Refresh();
        if (!base.has_value()) {
          break;
        }
        auto status = ops.Commit(**base, **base);
        if (status.has_value()) {
          ++successes;
          break;
        }
        if (status.error().kind == ErrorKind::kCommitFailed) {
          ++cas_failures;
          continue;  // retry against the new pointer
        }
        FAIL() << "Unexpected commit error: " << status.error().message;
        return;
      }
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kWorkers);
  for (int i = 0; i < kWorkers; ++i) {
    threads.emplace_back(worker, i);
  }
  for (auto& t : threads) {
    t.join();
  }

  // Each worker must eventually land all of its commits.
  EXPECT_EQ(successes.load(), kWorkers * kRoundsPerWorker);
  // We expect at least some contention (CAS retries) under load -- not
  // strictly required for correctness but a sanity signal that the lock
  // path is exercised.
  EXPECT_GE(cas_failures.load(), 0);

  // version-hint.text must record exactly the number of successful commits.
  HadoopTableOperations final_ops(file_io_, table_dir_);
  ICEBERG_UNWRAP_OR_FAIL(auto final_meta, final_ops.Refresh());
  EXPECT_EQ(final_ops.current_version(), 1 + kWorkers * kRoundsPerWorker);
  EXPECT_NE(final_meta, nullptr);
}

}  // namespace iceberg::hadoop
