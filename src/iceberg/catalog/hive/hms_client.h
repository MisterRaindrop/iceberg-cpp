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

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/catalog/hive/hive_catalog_properties.h"
#include "iceberg/catalog/hive/hive_utils.h"
#include "iceberg/catalog/hive/iceberg_hive_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/hive/hms_client.h
/// \brief Thin wrapper around the generated Hive Metastore Thrift client.
///
/// The header deliberately keeps Apache Thrift types out of the public
/// interface. All Thrift includes, transport / protocol setup and
/// generated-code references live in hms_client.cc behind a pImpl.

namespace iceberg::hive {

/// \brief A single host:port pair parsed from an HMS URI.
///
/// HMS URIs commonly take one of the forms:
///   * `thrift://host:port`        (Java HiveCatalog convention)
///   * `host:port`                 (iceberg-rust HmsCatalog convention)
///   * comma-separated list of either form for HA failover
///
/// `port` defaults to 9083 (Hive's well-known metastore port) when the
/// caller omits an explicit port.
struct ICEBERG_HIVE_EXPORT HmsEndpoint {
  std::string host;
  int port = 9083;
};

/// \brief Parse an HMS URI string into one or more endpoints.
///
/// Each comma-separated segment is treated as an independent endpoint;
/// surrounding whitespace and an optional `thrift://` scheme prefix are
/// stripped. Returns an InvalidArgument error if any segment fails to
/// produce a non-empty host and a port within (0, 65535].
ICEBERG_HIVE_EXPORT Result<std::vector<HmsEndpoint>> ParseHmsUris(std::string_view uri);

/// \brief Tunable timeouts for `HmsClient::LockExclusive`'s polling
/// loop. Mirrors Java's `hive.lock-check-min-wait-ms` /
/// `max-wait-ms` / `acquire-timeout-ms`.
struct ICEBERG_HIVE_EXPORT HmsLockOptions {
  int32_t check_min_wait_ms = 50;
  int32_t check_max_wait_ms = 5000;
  int32_t acquire_timeout_ms = 180000;
  int32_t heartbeat_interval_ms = 240000;
};

/// \brief A live connection to a Hive Metastore over Thrift.
///
/// The class encapsulates the TSocket / TTransport / TProtocol /
/// generated-client triple required by Apache Thrift's C++ runtime.
/// Construction goes through `Connect`, which performs URI parsing,
/// transport selection and an explicit `transport->open()` so that
/// configuration errors surface as `iceberg::Error` rather than C++
/// exceptions.
///
/// Thrift-call wrappers cover the full namespace (database) and table
/// CRUD surface plus EXCLUSIVE table-level locks. Each wrapper catches
/// the Thrift exceptions Hive can throw (`NoSuchObjectException`,
/// `MetaException`, etc.) and converts them to `iceberg::Error` via
/// `hive_errors.h` so the public interface stays Thrift-free.
///
/// A single `HmsClient` is **not thread-safe** -- Apache Thrift's
/// generated C++ client serialises requests and responses on one
/// transport buffer. Concurrent users must own their own client (the
/// usual path is via `HmsClientPool`, which is what `HiveCatalog` uses
/// internally so that `HiveCatalog` itself is safe to share across
/// threads).
class ICEBERG_HIVE_EXPORT HmsClient {
 public:
  ~HmsClient();

  HmsClient(const HmsClient&) = delete;
  HmsClient& operator=(const HmsClient&) = delete;
  HmsClient(HmsClient&&) = delete;
  HmsClient& operator=(HmsClient&&) = delete;

  /// \brief Connect to the Hive Metastore described by `config`.
  ///
  /// The first endpoint listed in `config.Uri()` is used; HA failover
  /// to subsequent endpoints is left to a future commit.
  static Result<std::unique_ptr<HmsClient>> Connect(const HiveCatalogProperties& config);

  /// \name Database (namespace) operations
  /// Wrappers around the HMS Thrift `*_database` calls. Each method
  /// catches the specific Thrift exception subclasses and forwards
  /// them to the factories in hive_errors.h, so callers see typed
  /// `iceberg::Error` rather than raw C++ exceptions.
  /// @{

  /// \brief List the names of every database visible to this HMS.
  Result<std::vector<std::string>> GetAllDatabases();

  /// \brief Load a single database by name.
  ///
  /// Returns `kNoSuchNamespace` when the database does not exist.
  Result<HiveDatabase> GetDatabase(std::string_view name);

  /// \brief Create the database described by `database`.
  ///
  /// Returns `kAlreadyExists` when a database with the same name is
  /// already present.
  Status CreateDatabase(const HiveDatabase& database);

  /// \brief Drop a database by name. `cascade` is forwarded directly to
  /// HMS; when false, dropping a non-empty database returns
  /// `kNotAllowed`.
  Status DropDatabase(std::string_view name, bool cascade);

  /// \brief Replace the database with `name` by `database`.
  Status AlterDatabase(std::string_view name, const HiveDatabase& database);

  /// @}

  /// \name Table operations
  /// Wrappers around the HMS Thrift `*_table` calls. The conversion
  /// between `HiveTable` and the generated Thrift `Table` lives in
  /// hms_client.cc so Thrift types never leak into the public API.
  /// @{

  /// \brief List the names of every table in `db_name`.
  Result<std::vector<std::string>> GetAllTables(std::string_view db_name);

  /// \brief Load a single table by (database, table) name pair.
  ///
  /// Returns `kNoSuchTable` when the table does not exist.
  Result<HiveTable> GetTable(std::string_view db_name, std::string_view table_name);

  /// \brief Create the table described by `table`.
  Status CreateTable(const HiveTable& table);

  /// \brief Drop the table identified by `db_name` and `table_name`.
  ///
  /// `delete_data` is forwarded to HMS; iceberg_hive normally passes
  /// false because data file lifecycle is managed via Iceberg snapshot
  /// expiry rather than HMS.
  Status DropTable(std::string_view db_name, std::string_view table_name,
                   bool delete_data);

  /// \brief Replace the existing table at (`db_name`, `table_name`)
  /// with `new_table`. Used both for renames (when `new_table.db_name`
  /// or `new_table.table_name` differs from the source) and for in-place
  /// updates (parameter / location / columns) via the commit path.
  Status AlterTable(std::string_view db_name, std::string_view table_name,
                    const HiveTable& new_table);

  /// @}

  /// \name HMS locks (optional commit-path guard)
  /// Wraps HMS `lock` / `unlock` so HiveTableOperations::Commit can
  /// serialise the GetTable -> AlterTable critical section when the
  /// catalog is configured with `hive.lock-enabled=true`.
  /// @{

  /// \brief Opaque handle returned by LockExclusive and consumed by
  /// Unlock. Default-constructed handles carry a sentinel lock_id and
  /// are treated as no-ops by Unlock.
  struct ICEBERG_HIVE_EXPORT HmsLockHandle {
    int64_t lock_id = -1;
    bool acquired() const { return lock_id >= 0; }
  };

  /// \brief Acquire an EXCLUSIVE TABLE-level lock on
  /// (`db_name`, `table_name`).
  ///
  /// On the initial `lock` call:
  ///   * `ACQUIRED`     -> returns the lock id immediately.
  ///   * `WAITING`      -> polls `check_lock` with exponential backoff
  ///                       between `check_min_wait_ms` and
  ///                       `check_max_wait_ms`, for at most
  ///                       `acquire_timeout_ms`. Returns the lock id
  ///                       once HMS reports `ACQUIRED`.
  ///   * `NOT_ACQUIRED` / `ABORT` / timeout -> the queued handle is
  ///                       released and `kCommitFailed` is returned so
  ///                       the caller's transaction loop can retry.
  Result<HmsLockHandle> LockExclusive(std::string_view db_name,
                                      std::string_view table_name,
                                      const HmsLockOptions& options = {});

  /// \brief Release a previously acquired lock. Idempotent for the
  /// sentinel handle returned by HmsLockHandle{}.
  Status Unlock(HmsLockHandle handle);

  /// \brief Send a single `heartbeat(0, lockid)` to HMS to keep the
  /// referenced lock from being reclaimed by HMS's server-side
  /// `txn.timeout`. Used by `HmsLockHeartbeat` from a background thread.
  Status Heartbeat(int64_t lock_id);

  /// @}

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  explicit HmsClient(std::unique_ptr<Impl> impl);
};

/// \brief RAII background thread that keeps an HMS lock alive while it
/// is held.
///
/// Java's `MetastoreLock` schedules a heartbeat task at
/// `heartbeat-interval / 2` so the HMS lock manager does not reclaim the
/// lock under server-side `txn.timeout`. We mirror that with one
/// dedicated `std::thread` per held lock. The heartbeater opens its own
/// dedicated `HmsClient` so it can issue RPCs without racing the commit
/// path that owns the original (pooled) client.
///
/// `Stop()` (and the destructor) signal the worker to exit and join
/// the thread. The destructor is best-effort: any `heartbeat` failures
/// are swallowed so a transient HMS hiccup does not abort the calling
/// commit; the CAS still protects correctness even if the heartbeat
/// missed.
class ICEBERG_HIVE_EXPORT HmsLockHeartbeat {
 public:
  /// \brief Start a heartbeat thread for `lock_id`. The thread connects
  /// to HMS via `HmsClient::Connect(config)` and fires
  /// `heartbeat(0, lock_id)` every `interval_ms / 2` until `Stop()` is
  /// called or the heartbeater is destroyed. If the dedicated HMS
  /// connection cannot be opened, `Start()` returns an error and no
  /// thread is created (the caller may proceed without heartbeats but
  /// the lock is then bounded by HMS's lease).
  static Result<std::unique_ptr<HmsLockHeartbeat>> Start(
      const HiveCatalogProperties& config, int64_t lock_id, int32_t interval_ms);

  ~HmsLockHeartbeat();

  HmsLockHeartbeat(const HmsLockHeartbeat&) = delete;
  HmsLockHeartbeat& operator=(const HmsLockHeartbeat&) = delete;
  HmsLockHeartbeat(HmsLockHeartbeat&&) = delete;
  HmsLockHeartbeat& operator=(HmsLockHeartbeat&&) = delete;

  /// \brief Signal the worker to stop and join the thread. Idempotent.
  void Stop();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  explicit HmsLockHeartbeat(std::unique_ptr<Impl> impl);
};

}  // namespace iceberg::hive
