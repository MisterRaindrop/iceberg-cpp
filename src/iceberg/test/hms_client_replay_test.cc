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

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include <gtest/gtest.h>

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
  void alter_table(const std::string&, const std::string&, const Table&) override {}
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

}  // namespace iceberg::hive
