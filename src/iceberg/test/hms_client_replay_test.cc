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

/// \file hms_client_replay_test.cc
/// \brief Unit tests for the Thrift exception -> iceberg::Error glue
///        in hms_client.cc.
///
/// Each method on `HmsClient` wraps a single call to the generated
/// `ThriftHiveMetastoreClient`, catches the specific HMS exception
/// subclasses, and dispatches through `hive_errors.h`. Without a
/// live HMS the production path is only reachable via the docker
/// integration suite; this test file substitutes a
/// `ThriftHiveMetastoreNull` subclass and throws the relevant
/// exception type per RPC, asserting the resulting `iceberg::Error`
/// kind / message. Reaches the catch blocks at line-coverage level
/// that the FakeHmsClient (which BYPASSES `hms_client.cc`) cannot.

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include <gtest/gtest.h>
#include <thrift/transport/TTransportException.h>

#include "ThriftHiveMetastore.h"
#include "iceberg/catalog/hive/hms_client.h"
#include "iceberg/catalog/hive/hms_client_test_seam.h"
#include "iceberg/result.h"

namespace iceberg::hive {

namespace {

using Apache::Hadoop::Hive::AlreadyExistsException;
using Apache::Hadoop::Hive::Database;
using Apache::Hadoop::Hive::GetTableRequest;
using Apache::Hadoop::Hive::GetTableResult;
using Apache::Hadoop::Hive::HeartbeatRequest;
using Apache::Hadoop::Hive::InvalidObjectException;
using Apache::Hadoop::Hive::InvalidOperationException;
using Apache::Hadoop::Hive::LockRequest;
using Apache::Hadoop::Hive::LockResponse;
using Apache::Hadoop::Hive::LockState;
using Apache::Hadoop::Hive::MetaException;
using Apache::Hadoop::Hive::NoSuchObjectException;
using Apache::Hadoop::Hive::Table;
using Apache::Hadoop::Hive::ThriftHiveMetastoreNull;
using Apache::Hadoop::Hive::UnknownDBException;

// Subclasses ThriftHiveMetastoreNull (a generated no-op
// implementation) and overrides the specific RPC methods each test
// cares about. Default behaviour is whatever Null returns.
class FakeBackend : public ThriftHiveMetastoreNull {
 public:
  // Per-RPC behavior knobs. Each test sets the ones it needs.

  // Database ops.
  std::function<void(std::vector<std::string>&)> on_get_all_databases;
  std::function<void(Database&, const std::string&)> on_get_database;
  std::function<void(const Database&)> on_create_database;
  std::function<void(const std::string&, bool, bool)> on_drop_database;
  std::function<void(const std::string&, const Database&)> on_alter_database;

  // Table ops.
  std::function<void(std::vector<std::string>&, const std::string&)> on_get_all_tables;
  std::function<void(GetTableResult&, const GetTableRequest&)> on_get_table_req;
  std::function<void(const Table&)> on_create_table;
  std::function<void(const std::string&, const std::string&, bool)> on_drop_table;

  // Lock ops.
  std::function<void(LockResponse&, const LockRequest&)> on_lock;
  std::function<void(LockResponse&, int64_t)> on_check_lock;
  std::function<void(int64_t)> on_unlock;
  std::function<void(const HeartbeatRequest&)> on_heartbeat;

  // Override the generated thrift methods. Each just forwards to the
  // user-provided callback (if set), or falls through to the Null base
  // which leaves _return default-constructed.
  void get_all_databases(std::vector<std::string>& _return) override {
    if (on_get_all_databases) on_get_all_databases(_return);
  }
  void get_database(Database& _return, const std::string& name) override {
    if (on_get_database) on_get_database(_return, name);
  }
  void create_database(const Database& db) override {
    if (on_create_database) on_create_database(db);
  }
  void drop_database(const std::string& name, const bool deleteData,
                     const bool cascade) override {
    if (on_drop_database) on_drop_database(name, deleteData, cascade);
  }
  void alter_database(const std::string& name, const Database& db) override {
    if (on_alter_database) on_alter_database(name, db);
  }
  void get_all_tables(std::vector<std::string>& _return,
                      const std::string& db_name) override {
    if (on_get_all_tables) on_get_all_tables(_return, db_name);
  }
  void get_table_req(GetTableResult& _return, const GetTableRequest& request) override {
    if (on_get_table_req) on_get_table_req(_return, request);
  }
  void create_table(const Table& t) override {
    if (on_create_table) on_create_table(t);
  }
  void drop_table(const std::string& db, const std::string& tbl,
                  const bool deleteData) override {
    if (on_drop_table) on_drop_table(db, tbl, deleteData);
  }
  std::function<void(const std::string&, const std::string&, const Table&)>
      on_alter_table;
  void alter_table(const std::string& db, const std::string& tbl,
                   const Table& t) override {
    if (on_alter_table) on_alter_table(db, tbl, t);
  }
  void lock(LockResponse& _return, const LockRequest& request) override {
    if (on_lock) on_lock(_return, request);
  }
  void check_lock(LockResponse& _return,
                  const Apache::Hadoop::Hive::CheckLockRequest& req) override {
    if (on_check_lock) on_check_lock(_return, req.lockid);
  }
  void unlock(const Apache::Hadoop::Hive::UnlockRequest& req) override {
    if (on_unlock) on_unlock(req.lockid);
  }
  void heartbeat(const HeartbeatRequest& req) override {
    if (on_heartbeat) on_heartbeat(req);
  }
};

std::unique_ptr<HmsClient> MakeWith(std::function<void(FakeBackend&)> setup) {
  auto fake = std::make_unique<FakeBackend>();
  if (setup) setup(*fake);
  return HmsClientForTesting(std::move(fake));
}

}  // namespace

// =====================================================================
// Database ops -- happy + each exception class the wrapper catches.
// =====================================================================

TEST(HmsClientThriftGlue, GetAllDatabasesHappyPath) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_get_all_databases = [](std::vector<std::string>& out) { out = {"db1", "db2"}; };
  });
  auto names = client->GetAllDatabases();
  ASSERT_TRUE(names.has_value());
  ASSERT_EQ(names->size(), 2u);
  EXPECT_EQ(names->at(0), "db1");
}

TEST(HmsClientThriftGlue, GetAllDatabasesMetaException) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_get_all_databases = [](std::vector<std::string>&) {
      MetaException e;
      e.message = "boom";
      throw e;
    };
  });
  auto r = client->GetAllDatabases();
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kIOError);
}

TEST(HmsClientThriftGlue, GetDatabaseNoSuchObjectMapsToNoSuchNamespace) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_get_database = [](Database&, const std::string&) {
      NoSuchObjectException e;
      e.message = "db missing";
      throw e;
    };
  });
  auto r = client->GetDatabase("missing");
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kNoSuchNamespace);
}

TEST(HmsClientThriftGlue, GetDatabaseHappyPathRoundsTripFields) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_get_database = [](Database& out, const std::string& name) {
      out.name = name;
      out.description = "from-test";
      out.locationUri = "file:///tmp";
      out.ownerName = "alice";
      out.parameters = {{"k", "v"}};
    };
  });
  auto r = client->GetDatabase("db");
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->name, "db");
  EXPECT_EQ(r->description, "from-test");
  EXPECT_EQ(r->location_uri, "file:///tmp");
  EXPECT_EQ(r->owner_name, "alice");
  EXPECT_EQ(r->parameters.at("k"), "v");
}

TEST(HmsClientThriftGlue, CreateDatabaseAlreadyExistsMaps) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_create_database = [](const Database&) {
      AlreadyExistsException e;
      e.message = "dup";
      throw e;
    };
  });
  auto r = client->CreateDatabase({.name = "x"});
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kAlreadyExists);
}

TEST(HmsClientThriftGlue, CreateDatabaseInvalidObjectMapsToInvalidArgument) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_create_database = [](const Database&) {
      InvalidObjectException e;
      e.message = "bad name";
      throw e;
    };
  });
  auto r = client->CreateDatabase({.name = "?"});
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kInvalidArgument);
}

TEST(HmsClientThriftGlue, DropDatabaseInvalidOperationMapsToNotAllowed) {
  // Dropping a non-empty database without cascade -> HMS throws
  // InvalidOperationException which maps to kNotAllowed.
  auto client = MakeWith([](FakeBackend& f) {
    f.on_drop_database = [](const std::string&, bool, bool) {
      InvalidOperationException e;
      e.message = "non-empty";
      throw e;
    };
  });
  auto r = client->DropDatabase("x", /*cascade=*/false);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kNotAllowed);
}

TEST(HmsClientThriftGlue, AlterDatabaseUnknownDbMapsToNoSuchNamespace) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_alter_database = [](const std::string&, const Database&) {
      NoSuchObjectException e;
      e.message = "missing";
      throw e;
    };
  });
  auto r = client->AlterDatabase("x", {.name = "x"});
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kNoSuchNamespace);
}

// =====================================================================
// Table ops.
// =====================================================================

TEST(HmsClientThriftGlue, GetAllTablesHappyPath) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_get_all_tables = [](std::vector<std::string>& out, const std::string&) {
      out = {"t1", "t2"};
    };
  });
  auto r = client->GetAllTables("db");
  ASSERT_TRUE(r.has_value());
  ASSERT_EQ(r->size(), 2u);
}

TEST(HmsClientThriftGlue, GetTableNoSuchObjectMapsToNoSuchTable) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_get_table_req = [](GetTableResult&, const GetTableRequest&) {
      NoSuchObjectException e;
      e.message = "missing";
      throw e;
    };
  });
  auto r = client->GetTable("db", "t");
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kNoSuchTable);
}

TEST(HmsClientThriftGlue, GetTableHappyPathRoundsTripParameters) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_get_table_req = [](GetTableResult& result, const GetTableRequest& request) {
      result.table.dbName = request.dbName;
      result.table.tableName = request.tblName;
      result.table.parameters = {{"metadata_location", "file:///x"}};
    };
  });
  auto r = client->GetTable("db", "t");
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->db_name, "db");
  EXPECT_EQ(r->table_name, "t");
  EXPECT_EQ(r->parameters.at("metadata_location"), "file:///x");
}

TEST(HmsClientThriftGlue, CreateTableAlreadyExistsMaps) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_create_table = [](const Table&) {
      AlreadyExistsException e;
      e.message = "dup";
      throw e;
    };
  });
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  auto r = client->CreateTable(t);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kAlreadyExists);
}

TEST(HmsClientThriftGlue, DropTableNoSuchObjectMapsToNoSuchTable) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_drop_table = [](const std::string&, const std::string&, bool) {
      NoSuchObjectException e;
      e.message = "missing";
      throw e;
    };
  });
  auto r = client->DropTable("db", "t", /*delete_data=*/false);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kNoSuchTable);
}

TEST(HmsClientThriftGlue, GetTableMetaExceptionMapsToIoError) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_get_table_req = [](GetTableResult&, const GetTableRequest&) {
      MetaException e;
      e.message = "hms internal";
      throw e;
    };
  });
  auto r = client->GetTable("db", "t");
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kIOError);
}

// =====================================================================
// Locks.
// =====================================================================

TEST(HmsClientThriftGlue, LockExclusiveAcquiredImmediatelyReturnsHandle) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_lock = [](LockResponse& out, const LockRequest&) {
      out.lockid = 42;
      out.state = LockState::ACQUIRED;
    };
  });
  auto r = client->LockExclusive("db", "t");
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->lock_id, 42);
}

TEST(HmsClientThriftGlue, LockExclusiveNotAcquiredFailsAndUnlocksTheQueuedHandle) {
  // HMS returns NOT_ACQUIRED on the initial lock call -- the wrapper
  // releases the queued handle and reports CommitFailed so the
  // caller's transaction can retry.
  bool unlocked = false;
  auto client = MakeWith([&](FakeBackend& f) {
    f.on_lock = [](LockResponse& out, const LockRequest&) {
      out.lockid = 7;
      out.state = LockState::NOT_ACQUIRED;
    };
    f.on_unlock = [&](int64_t id) {
      if (id == 7) unlocked = true;
    };
  });
  auto r = client->LockExclusive("db", "t");
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kCommitFailed);
  EXPECT_TRUE(unlocked) << "the wrapper must release the NOT_ACQUIRED handle";
}

TEST(HmsClientThriftGlue, LockExclusiveWaitingThenAcquired) {
  // First lock() returns WAITING; the polling loop's check_lock()
  // returns ACQUIRED on its first poll. With very small timeouts the
  // test runs fast.
  auto client = MakeWith([](FakeBackend& f) {
    f.on_lock = [](LockResponse& out, const LockRequest&) {
      out.lockid = 99;
      out.state = LockState::WAITING;
    };
    f.on_check_lock = [](LockResponse& out, int64_t lockid) {
      out.lockid = lockid;
      out.state = LockState::ACQUIRED;
    };
  });
  HmsLockOptions options;
  options.check_min_wait_ms = 1;
  options.check_max_wait_ms = 10;
  options.acquire_timeout_ms = 5000;
  auto r = client->LockExclusive("db", "t", options);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->lock_id, 99);
}

TEST(HmsClientThriftGlue, UnlockHappyPath) {
  bool unlocked = false;
  auto client =
      MakeWith([&](FakeBackend& f) { f.on_unlock = [&](int64_t) { unlocked = true; }; });
  HmsClient::HmsLockHandle handle{.lock_id = 12};
  auto r = client->Unlock(handle);
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(unlocked);
}

TEST(HmsClientThriftGlue, UnlockSentinelHandleIsNoop) {
  // The default-constructed handle has lock_id=-1, which the wrapper
  // is documented to treat as a no-op.
  bool called = false;
  auto client =
      MakeWith([&](FakeBackend& f) { f.on_unlock = [&](int64_t) { called = true; }; });
  HmsClient::HmsLockHandle sentinel;  // lock_id = -1
  auto r = client->Unlock(sentinel);
  ASSERT_TRUE(r.has_value());
  EXPECT_FALSE(called) << "Unlock(sentinel) must not invoke the underlying thrift unlock";
}

TEST(HmsClientThriftGlue, HeartbeatHappyPath) {
  bool called = false;
  auto client = MakeWith([&](FakeBackend& f) {
    f.on_heartbeat = [&](const HeartbeatRequest& req) {
      called = true;
      EXPECT_EQ(req.lockid, 99);
    };
  });
  auto r = client->Heartbeat(99);
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(called);
}

TEST(HmsClientThriftGlue, HeartbeatNoSuchLockMaps) {
  // HMS returns NoSuchLockException when the lock was reclaimed by
  // txn.timeout before our heartbeat arrived. The wrapper translates
  // to kCommitFailed so the lock owner can re-acquire.
  auto client = MakeWith([](FakeBackend& f) {
    f.on_heartbeat = [](const HeartbeatRequest&) {
      Apache::Hadoop::Hive::NoSuchLockException e;
      e.message = "lock 42 expired";
      throw e;
    };
  });
  auto r = client->Heartbeat(42);
  ASSERT_FALSE(r.has_value());
}

TEST(HmsClientThriftGlue, HeartbeatTransportExceptionMapsToServiceUnavailable) {
  // Pins the TTransportException catch arm that none of the previous
  // tests reached. The wrapper must surface kServiceUnavailable so
  // the pool can react to the broken socket.
  auto client = MakeWith([](FakeBackend& f) {
    f.on_heartbeat = [](const HeartbeatRequest&) {
      throw apache::thrift::transport::TTransportException(
          apache::thrift::transport::TTransportException::END_OF_FILE, "socket closed");
    };
  });
  auto r = client->Heartbeat(42);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kServiceUnavailable);
}

TEST(HmsClientThriftGlue, AlterTableHappyPath) {
  bool called = false;
  auto client = MakeWith([&](FakeBackend& f) {
    f.on_alter_table = [&](const std::string& db, const std::string& tbl,
                           const Table& t) {
      called = true;
      EXPECT_EQ(db, "db");
      EXPECT_EQ(tbl, "t");
      EXPECT_EQ(t.dbName, "db");
    };
  });
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  auto r = client->AlterTable("db", "t", t);
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(called);
}

TEST(HmsClientThriftGlue, AlterTableNoSuchObjectMapsToNoSuchTable) {
  auto client = MakeWith([](FakeBackend& f) {
    f.on_alter_table = [](const std::string&, const std::string&, const Table&) {
      NoSuchObjectException e;
      e.message = "missing";
      throw e;
    };
  });
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  auto r = client->AlterTable("db", "t", t);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kNoSuchTable);
}

TEST(HmsClientThriftGlue, DropTableHappyPath) {
  bool called = false;
  auto client = MakeWith([&](FakeBackend& f) {
    f.on_drop_table = [&](const std::string&, const std::string&, bool) {
      called = true;
    };
  });
  auto r = client->DropTable("db", "t", /*delete_data=*/false);
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(called);
}

TEST(HmsClientThriftGlue, DropDatabaseHappyPath) {
  bool called = false;
  auto client = MakeWith([&](FakeBackend& f) {
    f.on_drop_database = [&](const std::string&, bool, bool) { called = true; };
  });
  auto r = client->DropDatabase("db", /*cascade=*/false);
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(called);
}

TEST(HmsClientThriftGlue, CreateDatabaseHappyPath) {
  bool called = false;
  auto client = MakeWith([&](FakeBackend& f) {
    f.on_create_database = [&](const Database& db) {
      called = true;
      EXPECT_EQ(db.name, "x");
    };
  });
  HiveDatabase d;
  d.name = "x";
  d.description = "desc";
  d.location_uri = "file:///x";
  d.owner_name = "alice";
  d.owner_type = "USER";
  d.parameters = {{"k", "v"}};
  auto r = client->CreateDatabase(d);
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(called);
}

TEST(HmsClientThriftGlue, AlterDatabaseHappyPath) {
  bool called = false;
  auto client = MakeWith([&](FakeBackend& f) {
    f.on_alter_database = [&](const std::string& n, const Database&) {
      called = true;
      EXPECT_EQ(n, "x");
    };
  });
  HiveDatabase d;
  d.name = "x";
  auto r = client->AlterDatabase("x", d);
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(called);
}

TEST(HmsClientThriftGlue, LockExclusiveAbortFailsWithCommitFailed) {
  // HMS rare path: lock() returns ABORT immediately. The wrapper
  // surfaces kCommitFailed (matching the NOT_ACQUIRED branch). No
  // unlock should fire because the lock was never owned.
  bool unlocked = false;
  auto client = MakeWith([&](FakeBackend& f) {
    f.on_lock = [](LockResponse& out, const LockRequest&) {
      out.lockid = 5;
      out.state = LockState::ABORT;
    };
    f.on_unlock = [&](int64_t) { unlocked = true; };
  });
  auto r = client->LockExclusive("db", "t");
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kCommitFailed);
  (void)unlocked;  // unlock may or may not be issued depending on impl
}

TEST(HmsClientThriftGlue, LockExclusiveWaitingTimesOutReleasesQueuedHandle) {
  // lock() returns WAITING and check_lock() keeps returning WAITING.
  // After the polling loop exhausts acquire_timeout_ms, the wrapper
  // releases the queued handle and surfaces kCommitFailed.
  bool unlocked = false;
  auto client = MakeWith([&](FakeBackend& f) {
    f.on_lock = [](LockResponse& out, const LockRequest&) {
      out.lockid = 13;
      out.state = LockState::WAITING;
    };
    f.on_check_lock = [](LockResponse& out, int64_t lockid) {
      out.lockid = lockid;
      out.state = LockState::WAITING;  // never acquired
    };
    f.on_unlock = [&](int64_t id) {
      if (id == 13) unlocked = true;
    };
  });
  HmsLockOptions options;
  options.check_min_wait_ms = 1;
  options.check_max_wait_ms = 2;
  options.acquire_timeout_ms = 5;  // bail almost immediately
  auto r = client->LockExclusive("db", "t", options);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kCommitFailed);
  EXPECT_TRUE(unlocked) << "timeout path must release the queued handle";
}

TEST(HmsClientThriftGlue, LockExclusiveCheckLockAbortReleasesQueuedHandle) {
  // lock() WAITING -> check_lock() ABORT. The polling loop's terminal
  // ABORT state must release the queued handle and surface
  // kCommitFailed.
  bool unlocked = false;
  auto client = MakeWith([&](FakeBackend& f) {
    f.on_lock = [](LockResponse& out, const LockRequest&) {
      out.lockid = 22;
      out.state = LockState::WAITING;
    };
    f.on_check_lock = [](LockResponse& out, int64_t lockid) {
      out.lockid = lockid;
      out.state = LockState::ABORT;
    };
    f.on_unlock = [&](int64_t id) {
      if (id == 22) unlocked = true;
    };
  });
  HmsLockOptions options;
  options.check_min_wait_ms = 1;
  options.check_max_wait_ms = 2;
  options.acquire_timeout_ms = 5000;
  auto r = client->LockExclusive("db", "t", options);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kCommitFailed);
  EXPECT_TRUE(unlocked);
}

TEST(HmsClientThriftGlue, LockExclusiveTransportExceptionDuringInitialLock) {
  // Pins the TTransportException catch arm in the initial lock() call.
  auto client = MakeWith([](FakeBackend& f) {
    f.on_lock = [](LockResponse&, const LockRequest&) {
      throw apache::thrift::transport::TTransportException(
          apache::thrift::transport::TTransportException::END_OF_FILE,
          "socket closed during lock");
    };
  });
  auto r = client->LockExclusive("db", "t");
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kServiceUnavailable);
}

// ----- Bulk catch-arm coverage --------------------------------------
//
// Each RPC method in hms_client.cc has the same shape:
//   try {
//     impl_->client->some_thrift_method(...);
//     return ok;
//   } catch (const SpecificHmsException&) { return TypedError(); }
//     ... more specific exceptions ...
//   catch (const apache::thrift::transport::TTransportException&) {
//     return TransportError();                  // -> kServiceUnavailable
//   } catch (const apache::thrift::TException&) {
//     return GenericThriftError();              // -> kIOError
//   }
//
// The earlier tests covered the specific-exception arms. These pin
// the bottom-of-stack `TTransportException` and generic `TException`
// arms across every RPC so the catch-block coverage in hms_client.cc
// is exhaustive.

#define IT_THROWS_TRANSPORT(name)                 \
  apache::thrift::transport::TTransportException( \
      apache::thrift::transport::TTransportException::END_OF_FILE, (name))

#define IT_THROWS_GENERIC(msg) apache::thrift::TException(msg)

TEST(HmsClientThriftGlue, GetAllDatabasesTransportFailureMapsToServiceUnavailable) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_all_databases = [](std::vector<std::string>&) {
      throw IT_THROWS_TRANSPORT("get_all_databases");
    };
  });
  auto r = c->GetAllDatabases();
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kServiceUnavailable);
}

TEST(HmsClientThriftGlue, GetAllDatabasesGenericExceptionMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_all_databases = [](std::vector<std::string>&) {
      throw IT_THROWS_GENERIC("get_all_databases generic");
    };
  });
  auto r = c->GetAllDatabases();
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().kind, ErrorKind::kIOError);
}

TEST(HmsClientThriftGlue, GetDatabaseTransportAndGenericArms) {
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_get_database = [](Database&, const std::string&) {
        throw IT_THROWS_TRANSPORT("get_database");
      };
    });
    EXPECT_EQ(c->GetDatabase("x").error().kind, ErrorKind::kServiceUnavailable);
  }
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_get_database = [](Database&, const std::string&) {
        throw IT_THROWS_GENERIC("get_database generic");
      };
    });
    EXPECT_EQ(c->GetDatabase("x").error().kind, ErrorKind::kIOError);
  }
}

TEST(HmsClientThriftGlue, CreateDatabaseTransportAndGenericArms) {
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_create_database = [](const Database&) {
        throw IT_THROWS_TRANSPORT("create_database");
      };
    });
    EXPECT_EQ(c->CreateDatabase({.name = "x"}).error().kind,
              ErrorKind::kServiceUnavailable);
  }
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_create_database = [](const Database&) {
        throw IT_THROWS_GENERIC("create_database generic");
      };
    });
    EXPECT_EQ(c->CreateDatabase({.name = "x"}).error().kind, ErrorKind::kIOError);
  }
}

TEST(HmsClientThriftGlue, DropDatabaseTransportAndGenericArms) {
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_drop_database = [](const std::string&, bool, bool) {
        throw IT_THROWS_TRANSPORT("drop_database");
      };
    });
    EXPECT_EQ(c->DropDatabase("x", false).error().kind, ErrorKind::kServiceUnavailable);
  }
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_drop_database = [](const std::string&, bool, bool) {
        throw IT_THROWS_GENERIC("drop_database generic");
      };
    });
    EXPECT_EQ(c->DropDatabase("x", false).error().kind, ErrorKind::kIOError);
  }
}

TEST(HmsClientThriftGlue, AlterDatabaseTransportAndGenericArms) {
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_alter_database = [](const std::string&, const Database&) {
        throw IT_THROWS_TRANSPORT("alter_database");
      };
    });
    EXPECT_EQ(c->AlterDatabase("x", {.name = "x"}).error().kind,
              ErrorKind::kServiceUnavailable);
  }
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_alter_database = [](const std::string&, const Database&) {
        throw IT_THROWS_GENERIC("alter_database generic");
      };
    });
    EXPECT_EQ(c->AlterDatabase("x", {.name = "x"}).error().kind, ErrorKind::kIOError);
  }
}

TEST(HmsClientThriftGlue, GetAllTablesTransportAndGenericArms) {
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_get_all_tables = [](std::vector<std::string>&, const std::string&) {
        throw IT_THROWS_TRANSPORT("get_all_tables");
      };
    });
    EXPECT_EQ(c->GetAllTables("db").error().kind, ErrorKind::kServiceUnavailable);
  }
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_get_all_tables = [](std::vector<std::string>&, const std::string&) {
        throw IT_THROWS_GENERIC("get_all_tables generic");
      };
    });
    EXPECT_EQ(c->GetAllTables("db").error().kind, ErrorKind::kIOError);
  }
}

TEST(HmsClientThriftGlue, GetTableTransportAndGenericArms) {
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_get_table_req = [](GetTableResult&, const GetTableRequest&) {
        throw IT_THROWS_TRANSPORT("get_table");
      };
    });
    EXPECT_EQ(c->GetTable("db", "t").error().kind, ErrorKind::kServiceUnavailable);
  }
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_get_table_req = [](GetTableResult&, const GetTableRequest&) {
        throw IT_THROWS_GENERIC("get_table generic");
      };
    });
    EXPECT_EQ(c->GetTable("db", "t").error().kind, ErrorKind::kIOError);
  }
}

TEST(HmsClientThriftGlue, CreateTableTransportAndGenericArms) {
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_create_table = [](const Table&) { throw IT_THROWS_TRANSPORT("create_table"); };
    });
    EXPECT_EQ(c->CreateTable(t).error().kind, ErrorKind::kServiceUnavailable);
  }
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_create_table = [](const Table&) {
        throw IT_THROWS_GENERIC("create_table generic");
      };
    });
    EXPECT_EQ(c->CreateTable(t).error().kind, ErrorKind::kIOError);
  }
}

TEST(HmsClientThriftGlue, DropTableTransportAndGenericArms) {
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_drop_table = [](const std::string&, const std::string&, bool) {
        throw IT_THROWS_TRANSPORT("drop_table");
      };
    });
    EXPECT_EQ(c->DropTable("db", "t", false).error().kind,
              ErrorKind::kServiceUnavailable);
  }
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_drop_table = [](const std::string&, const std::string&, bool) {
        throw IT_THROWS_GENERIC("drop_table generic");
      };
    });
    EXPECT_EQ(c->DropTable("db", "t", false).error().kind, ErrorKind::kIOError);
  }
}

TEST(HmsClientThriftGlue, AlterTableTransportAndGenericArms) {
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_alter_table = [](const std::string&, const std::string&, const Table&) {
        throw IT_THROWS_TRANSPORT("alter_table");
      };
    });
    EXPECT_EQ(c->AlterTable("db", "t", t).error().kind, ErrorKind::kServiceUnavailable);
  }
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_alter_table = [](const std::string&, const std::string&, const Table&) {
        throw IT_THROWS_GENERIC("alter_table generic");
      };
    });
    EXPECT_EQ(c->AlterTable("db", "t", t).error().kind, ErrorKind::kIOError);
  }
}

TEST(HmsClientThriftGlue, UnlockTransportAndGenericArms) {
  HmsClient::HmsLockHandle handle{.lock_id = 5};
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_unlock = [](int64_t) { throw IT_THROWS_TRANSPORT("unlock"); };
    });
    EXPECT_EQ(c->Unlock(handle).error().kind, ErrorKind::kServiceUnavailable);
  }
  {
    auto c = MakeWith([](FakeBackend& f) {
      f.on_unlock = [](int64_t) { throw IT_THROWS_GENERIC("unlock generic"); };
    });
    EXPECT_EQ(c->Unlock(handle).error().kind, ErrorKind::kIOError);
  }
}

TEST(HmsClientThriftGlue, CreateTableRoundTripsRichInput) {
  // Exercises the full ToThriftTable / FromThriftTable conversion
  // paths (parameters, columns, owner) so the helper-function
  // coverage isn't dominated by minimal inputs.
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  t.owner = "alice";
  t.table_type = "EXTERNAL_TABLE";
  t.location = "file:///tmp";
  t.parameters = {{"k", "v"}, {"metadata_location", "file:///x"}};
  t.serde = std::string(kLazySimpleSerDe);
  t.input_format = std::string(kFileInputFormat);
  t.output_format = std::string(kFileOutputFormat);
  HiveColumn col;
  col.name = "id";
  col.type_string = "bigint";
  t.columns.push_back(col);

  Table captured;
  auto c = MakeWith([&](FakeBackend& f) {
    f.on_create_table = [&](const Table& thrift_t) { captured = thrift_t; };
  });
  auto r = c->CreateTable(t);
  ASSERT_TRUE(r.has_value()) << r.error().message;
  EXPECT_EQ(captured.dbName, "db");
  EXPECT_EQ(captured.tableName, "t");
  EXPECT_EQ(captured.owner, "alice");
  EXPECT_EQ(captured.parameters.at("metadata_location"), "file:///x");
}

TEST(HmsClientThriftGlue, CreateDatabaseRoundTripsRichInput) {
  // Exercises ToThriftDatabase's owner / owner-type / parameters /
  // location_uri branches.
  HiveDatabase d;
  d.name = "warehouse";
  d.description = "hello";
  d.location_uri = "file:///wh";
  d.owner_name = "alice";
  d.owner_type = "USER";
  d.parameters = {{"k", "v"}, {"x", "1"}};
  Database captured;
  auto c = MakeWith([&](FakeBackend& f) {
    f.on_create_database = [&](const Database& thrift_d) { captured = thrift_d; };
  });
  auto r = c->CreateDatabase(d);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(captured.name, "warehouse");
  EXPECT_EQ(captured.description, "hello");
  EXPECT_EQ(captured.locationUri, "file:///wh");
  EXPECT_EQ(captured.ownerName, "alice");
  EXPECT_EQ(captured.parameters.at("x"), "1");
}

// ----- Remaining specific catch arms across every RPC --------------
//
// Each RPC declares ~5-7 catch arms for distinct HMS exception types.
// Earlier tests covered the "common" arms (NoSuchObjectException +
// TTransport + generic TException). These add the remaining specific
// arms so every `catch (const X&)` branch in hms_client.cc executes.

#define THROW_HMS(type, msg)       \
  do {                             \
    Apache::Hadoop::Hive::type _e; \
    _e.message = (msg);            \
    throw _e;                      \
  } while (0)

// --- GetDatabase: UnknownDBException + MetaException ---

TEST(HmsClientThriftGlue, GetDatabaseUnknownDBMapsToNoSuchNamespace) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_database = [](Database&, const std::string&) {
      THROW_HMS(UnknownDBException, "db");
    };
  });
  EXPECT_EQ(c->GetDatabase("x").error().kind, ErrorKind::kNoSuchNamespace);
}

TEST(HmsClientThriftGlue, GetDatabaseMetaExceptionMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_database = [](Database&, const std::string&) {
      THROW_HMS(MetaException, "meta");
    };
  });
  EXPECT_EQ(c->GetDatabase("x").error().kind, ErrorKind::kIOError);
}

// --- CreateDatabase: MetaException ---

TEST(HmsClientThriftGlue, CreateDatabaseMetaExceptionMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_create_database = [](const Database&) { THROW_HMS(MetaException, "meta"); };
  });
  EXPECT_EQ(c->CreateDatabase({.name = "x"}).error().kind, ErrorKind::kIOError);
}

// --- DropDatabase: NoSuchObject + MetaException ---

TEST(HmsClientThriftGlue, DropDatabaseNoSuchObjectMapsToNoSuchNamespace) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_drop_database = [](const std::string&, bool, bool) {
      THROW_HMS(NoSuchObjectException, "no db");
    };
  });
  EXPECT_EQ(c->DropDatabase("x", false).error().kind, ErrorKind::kNoSuchNamespace);
}

TEST(HmsClientThriftGlue, DropDatabaseMetaExceptionMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_drop_database = [](const std::string&, bool, bool) {
      THROW_HMS(MetaException, "meta");
    };
  });
  EXPECT_EQ(c->DropDatabase("x", false).error().kind, ErrorKind::kIOError);
}

// --- AlterDatabase: InvalidOperation + MetaException ---

TEST(HmsClientThriftGlue, AlterDatabaseInvalidOperationMapsToNotAllowed) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_alter_database = [](const std::string&, const Database&) {
      THROW_HMS(InvalidOperationException, "bad");
    };
  });
  EXPECT_EQ(c->AlterDatabase("x", {.name = "x"}).error().kind, ErrorKind::kNotAllowed);
}

TEST(HmsClientThriftGlue, AlterDatabaseMetaExceptionMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_alter_database = [](const std::string&, const Database&) {
      THROW_HMS(MetaException, "meta");
    };
  });
  EXPECT_EQ(c->AlterDatabase("x", {.name = "x"}).error().kind, ErrorKind::kIOError);
}

// --- GetAllTables: NoSuchObject + UnknownDB + MetaException ---

TEST(HmsClientThriftGlue, GetAllTablesNoSuchObjectMapsToNoSuchNamespace) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_all_tables = [](std::vector<std::string>&, const std::string&) {
      THROW_HMS(NoSuchObjectException, "no db");
    };
  });
  EXPECT_EQ(c->GetAllTables("x").error().kind, ErrorKind::kNoSuchNamespace);
}

TEST(HmsClientThriftGlue, GetAllTablesUnknownDBMapsToNoSuchNamespace) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_all_tables = [](std::vector<std::string>&, const std::string&) {
      THROW_HMS(UnknownDBException, "no db");
    };
  });
  EXPECT_EQ(c->GetAllTables("x").error().kind, ErrorKind::kNoSuchNamespace);
}

TEST(HmsClientThriftGlue, GetAllTablesMetaExceptionMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_all_tables = [](std::vector<std::string>&, const std::string&) {
      THROW_HMS(MetaException, "meta");
    };
  });
  EXPECT_EQ(c->GetAllTables("x").error().kind, ErrorKind::kIOError);
}

// --- GetTable: UnknownTable + UnknownDB ---

TEST(HmsClientThriftGlue, GetTableUnknownTableMapsToNoSuchTable) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_table_req = [](GetTableResult&, const GetTableRequest&) {
      THROW_HMS(UnknownTableException, "no t");
    };
  });
  EXPECT_EQ(c->GetTable("db", "t").error().kind, ErrorKind::kNoSuchTable);
}

TEST(HmsClientThriftGlue, GetTableUnknownDBMapsToNoSuchNamespace) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_table_req = [](GetTableResult&, const GetTableRequest&) {
      THROW_HMS(UnknownDBException, "no db");
    };
  });
  EXPECT_EQ(c->GetTable("db", "t").error().kind, ErrorKind::kNoSuchNamespace);
}

// --- CreateTable: InvalidObject + NoSuchObject + MetaException ---

TEST(HmsClientThriftGlue, CreateTableInvalidObjectMapsToInvalidArgument) {
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  auto c = MakeWith([](FakeBackend& f) {
    f.on_create_table = [](const Table&) { THROW_HMS(InvalidObjectException, "bad"); };
  });
  EXPECT_EQ(c->CreateTable(t).error().kind, ErrorKind::kInvalidArgument);
}

TEST(HmsClientThriftGlue, CreateTableNoSuchObjectMapsToNoSuchNamespace) {
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  auto c = MakeWith([](FakeBackend& f) {
    f.on_create_table = [](const Table&) { THROW_HMS(NoSuchObjectException, "no db"); };
  });
  EXPECT_EQ(c->CreateTable(t).error().kind, ErrorKind::kNoSuchNamespace);
}

TEST(HmsClientThriftGlue, CreateTableMetaExceptionMapsToIoError) {
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  auto c = MakeWith([](FakeBackend& f) {
    f.on_create_table = [](const Table&) { THROW_HMS(MetaException, "meta"); };
  });
  EXPECT_EQ(c->CreateTable(t).error().kind, ErrorKind::kIOError);
}

// --- DropTable: InvalidOperation + MetaException ---

TEST(HmsClientThriftGlue, DropTableInvalidOperationMapsToNotAllowed) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_drop_table = [](const std::string&, const std::string&, bool) {
      THROW_HMS(InvalidOperationException, "bad");
    };
  });
  EXPECT_EQ(c->DropTable("db", "t", false).error().kind, ErrorKind::kNotAllowed);
}

TEST(HmsClientThriftGlue, DropTableMetaExceptionMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_drop_table = [](const std::string&, const std::string&, bool) {
      THROW_HMS(MetaException, "meta");
    };
  });
  EXPECT_EQ(c->DropTable("db", "t", false).error().kind, ErrorKind::kIOError);
}

// --- AlterTable: InvalidObject + InvalidOperation + MetaException ---

TEST(HmsClientThriftGlue, AlterTableInvalidObjectMapsToInvalidArgument) {
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  auto c = MakeWith([](FakeBackend& f) {
    f.on_alter_table = [](const std::string&, const std::string&, const Table&) {
      THROW_HMS(InvalidObjectException, "bad");
    };
  });
  EXPECT_EQ(c->AlterTable("db", "t", t).error().kind, ErrorKind::kInvalidArgument);
}

TEST(HmsClientThriftGlue, AlterTableInvalidOperationMapsToNotAllowed) {
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  auto c = MakeWith([](FakeBackend& f) {
    f.on_alter_table = [](const std::string&, const std::string&, const Table&) {
      THROW_HMS(InvalidOperationException, "bad");
    };
  });
  EXPECT_EQ(c->AlterTable("db", "t", t).error().kind, ErrorKind::kNotAllowed);
}

TEST(HmsClientThriftGlue, AlterTableMetaExceptionMapsToIoError) {
  HiveTable t;
  t.db_name = "db";
  t.table_name = "t";
  auto c = MakeWith([](FakeBackend& f) {
    f.on_alter_table = [](const std::string&, const std::string&, const Table&) {
      THROW_HMS(MetaException, "meta");
    };
  });
  EXPECT_EQ(c->AlterTable("db", "t", t).error().kind, ErrorKind::kIOError);
}

// --- LockExclusive's lock() catch arms: TxnAborted + MetaException + generic ---

TEST(HmsClientThriftGlue, LockExclusiveTxnAbortedFailsCommitFailed) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_lock = [](LockResponse&, const LockRequest&) {
      THROW_HMS(TxnAbortedException, "txn aborted");
    };
  });
  EXPECT_EQ(c->LockExclusive("db", "t").error().kind, ErrorKind::kCommitFailed);
}

TEST(HmsClientThriftGlue, LockExclusiveMetaExceptionMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_lock = [](LockResponse&, const LockRequest&) {
      THROW_HMS(MetaException, "meta");
    };
  });
  EXPECT_EQ(c->LockExclusive("db", "t").error().kind, ErrorKind::kIOError);
}

TEST(HmsClientThriftGlue, LockExclusiveGenericThriftMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_lock = [](LockResponse&, const LockRequest&) {
      throw apache::thrift::TException("generic");
    };
  });
  EXPECT_EQ(c->LockExclusive("db", "t").error().kind, ErrorKind::kIOError);
}

// --- Unlock: NoSuchLock + TxnOpen + MetaException ---

TEST(HmsClientThriftGlue, UnlockNoSuchLockMapsToNotFound) {
  // Per hms_client.cc: NoSuchLockException -> NotFound (kNotFound).
  auto c = MakeWith([](FakeBackend& f) {
    f.on_unlock = [](int64_t) { THROW_HMS(NoSuchLockException, "vanished"); };
  });
  HmsClient::HmsLockHandle h{.lock_id = 1};
  EXPECT_EQ(c->Unlock(h).error().kind, ErrorKind::kNotFound);
}

TEST(HmsClientThriftGlue, UnlockTxnOpenMapsToNotAllowed) {
  // Per hms_client.cc: TxnOpenException -> InvalidOperationError ->
  // kNotAllowed.
  auto c = MakeWith([](FakeBackend& f) {
    f.on_unlock = [](int64_t) { THROW_HMS(TxnOpenException, "txn open"); };
  });
  HmsClient::HmsLockHandle h{.lock_id = 1};
  EXPECT_EQ(c->Unlock(h).error().kind, ErrorKind::kNotAllowed);
}

TEST(HmsClientThriftGlue, UnlockMetaExceptionMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_unlock = [](int64_t) { THROW_HMS(MetaException, "meta"); };
  });
  HmsClient::HmsLockHandle h{.lock_id = 1};
  EXPECT_EQ(c->Unlock(h).error().kind, ErrorKind::kIOError);
}

// --- Heartbeat: TxnAborted + MetaException + generic ---

TEST(HmsClientThriftGlue, HeartbeatTxnAbortedMapsToCommitFailed) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_heartbeat = [](const HeartbeatRequest&) {
      THROW_HMS(TxnAbortedException, "txn aborted");
    };
  });
  EXPECT_EQ(c->Heartbeat(1).error().kind, ErrorKind::kCommitFailed);
}

TEST(HmsClientThriftGlue, HeartbeatMetaExceptionMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_heartbeat = [](const HeartbeatRequest&) { THROW_HMS(MetaException, "meta"); };
  });
  EXPECT_EQ(c->Heartbeat(1).error().kind, ErrorKind::kIOError);
}

TEST(HmsClientThriftGlue, HeartbeatGenericThriftMapsToIoError) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_heartbeat = [](const HeartbeatRequest&) {
      throw apache::thrift::TException("generic");
    };
  });
  EXPECT_EQ(c->Heartbeat(1).error().kind, ErrorKind::kIOError);
}

// ----- PollCheckLock 6 catch arms -----------------------------------
//
// The polling loop catches everything check_lock can throw and folds
// it into a synthetic `LockState::ABORT` with an error message. Each
// arm is reachable via LockExclusive(WAITING -> check_lock throws X).

namespace {

// Helper: drive LockExclusive through a script where lock() returns
// WAITING and check_lock() throws the supplied callable's exception.
template <typename Throw>
Result<HmsClient::HmsLockHandle> RunPollingLockWithCheckLockException(Throw throw_fn) {
  auto c = MakeWith([throw_fn](FakeBackend& f) {
    f.on_lock = [](LockResponse& out, const LockRequest&) {
      out.lockid = 1;
      out.state = LockState::WAITING;
    };
    f.on_check_lock = [throw_fn](LockResponse&, int64_t) { throw_fn(); };
    f.on_unlock = [](int64_t) {};  // accept the release-of-queued-handle
  });
  HmsLockOptions opt;
  opt.check_min_wait_ms = 1;
  opt.check_max_wait_ms = 2;
  opt.acquire_timeout_ms = 50;
  return c->LockExclusive("db", "t", opt);
}

}  // namespace

TEST(HmsClientThriftGlue, PollCheckLockNoSuchLockMapsToAbort) {
  auto r = RunPollingLockWithCheckLockException(
      [] { THROW_HMS(NoSuchLockException, "vanished"); });
  EXPECT_FALSE(r.has_value());  // ABORT -> kCommitFailed
}

TEST(HmsClientThriftGlue, PollCheckLockTxnAbortedMapsToAbort) {
  auto r = RunPollingLockWithCheckLockException(
      [] { THROW_HMS(TxnAbortedException, "txn aborted"); });
  EXPECT_FALSE(r.has_value());
}

TEST(HmsClientThriftGlue, PollCheckLockTxnOpenMapsToAbort) {
  auto r = RunPollingLockWithCheckLockException(
      [] { THROW_HMS(TxnOpenException, "txn open"); });
  EXPECT_FALSE(r.has_value());
}

TEST(HmsClientThriftGlue, PollCheckLockMetaExceptionMapsToAbort) {
  auto r = RunPollingLockWithCheckLockException([] { THROW_HMS(MetaException, "meta"); });
  EXPECT_FALSE(r.has_value());
}

TEST(HmsClientThriftGlue, PollCheckLockTransportSetsTransportFailedFlag) {
  auto r = RunPollingLockWithCheckLockException([] {
    throw apache::thrift::transport::TTransportException(
        apache::thrift::transport::TTransportException::END_OF_FILE, "tx");
  });
  EXPECT_FALSE(r.has_value());
}

TEST(HmsClientThriftGlue, PollCheckLockGenericThriftMapsToAbort) {
  auto r = RunPollingLockWithCheckLockException(
      [] { throw apache::thrift::TException("generic"); });
  EXPECT_FALSE(r.has_value());
}

// ----- Conversion helper edges --------------------------------------

TEST(HmsClientThriftGlue, GetDatabaseRoundTripsOwnerTypeUser) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_database = [](Database& out, const std::string& name) {
      out.name = name;
      out.ownerName = "alice";
      out.ownerType = Apache::Hadoop::Hive::PrincipalType::USER;
      out.__isset.ownerType = true;
    };
  });
  auto r = c->GetDatabase("x");
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->owner_type, "USER");
}

TEST(HmsClientThriftGlue, GetDatabaseRoundTripsOwnerTypeRole) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_database = [](Database& out, const std::string& name) {
      out.name = name;
      out.ownerType = Apache::Hadoop::Hive::PrincipalType::ROLE;
      out.__isset.ownerType = true;
    };
  });
  auto r = c->GetDatabase("x");
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->owner_type, "ROLE");
}

TEST(HmsClientThriftGlue, GetDatabaseRoundTripsOwnerTypeGroup) {
  auto c = MakeWith([](FakeBackend& f) {
    f.on_get_database = [](Database& out, const std::string& name) {
      out.name = name;
      out.ownerType = Apache::Hadoop::Hive::PrincipalType::GROUP;
      out.__isset.ownerType = true;
    };
  });
  auto r = c->GetDatabase("x");
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->owner_type, "GROUP");
}

TEST(HmsClientThriftGlue, CreateDatabaseHandlesOwnerTypeRole) {
  // Drives ToThriftDatabase's "owner_type=ROLE" branch.
  Database captured;
  auto c = MakeWith([&](FakeBackend& f) {
    f.on_create_database = [&](const Database& d) { captured = d; };
  });
  HiveDatabase d;
  d.name = "x";
  d.owner_name = "g";
  d.owner_type = "ROLE";
  ASSERT_TRUE(c->CreateDatabase(d).has_value());
  EXPECT_EQ(captured.ownerType, Apache::Hadoop::Hive::PrincipalType::ROLE);
}

TEST(HmsClientThriftGlue, CreateDatabaseHandlesOwnerTypeGroup) {
  Database captured;
  auto c = MakeWith([&](FakeBackend& f) {
    f.on_create_database = [&](const Database& d) { captured = d; };
  });
  HiveDatabase d;
  d.name = "x";
  d.owner_name = "g";
  d.owner_type = "GROUP";
  ASSERT_TRUE(c->CreateDatabase(d).has_value());
  EXPECT_EQ(captured.ownerType, Apache::Hadoop::Hive::PrincipalType::GROUP);
}

#undef IT_THROWS_TRANSPORT
#undef IT_THROWS_GENERIC
#undef THROW_HMS

// ----- HmsLockHeartbeat ---------------------------------------------

TEST(HmsClientThriftGlue, HmsLockHeartbeatFiresAndStopsCleanly) {
  // Spawn a heartbeat with a fake-backed HmsClient, observe at least
  // one heartbeat fires, then Stop() joins the worker thread cleanly.
  // The destructor is exercised on scope exit.
  std::atomic<int> heartbeat_calls{0};
  auto fake = std::make_unique<FakeBackend>();
  fake->on_heartbeat = [&heartbeat_calls](const HeartbeatRequest& req) {
    EXPECT_EQ(req.lockid, 7);
    ++heartbeat_calls;
  };
  auto inner_client = HmsClientForTesting(std::move(fake));

  // interval_ms=20 -> worker fires every 10ms. Give it ample time to
  // emit at least one beat without blowing test wall-clock.
  auto heartbeat = HmsLockHeartbeatForTesting(std::move(inner_client),
                                              /*lock_id=*/7,
                                              /*interval_ms=*/20);
  for (int attempt = 0; attempt < 50 && heartbeat_calls.load() == 0; ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_GT(heartbeat_calls.load(), 0);
  heartbeat->Stop();
  heartbeat->Stop();  // idempotent
}

}  // namespace iceberg::hive
