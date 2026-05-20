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

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <utility>

#include "iceberg/catalog/hive/hive_catalog_properties.h"
#include "iceberg/catalog/hive/hms_client.h"
#include "iceberg/catalog/hive/iceberg_hive_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/hive/hms_client_pool.h
/// \brief Thread-safe pool of `HmsClient` instances, modelled on Java's
///        `CachedClientPool`.

namespace iceberg::hive {

/// \brief Fixed-size pool of `HmsClient` connections.
///
/// `HmsClient` itself wraps a single, non-thread-safe Thrift socket. To
/// let `HiveCatalog` serve concurrent callers without serialising every
/// RPC through one socket, the catalog holds a pool of clients and
/// checks one out for each catalog-level method (matching Java's
/// `CachedClientPool.run(...)` pattern).
///
/// Clients are created lazily on first checkout until the pool reaches
/// `hive.client-pool-size`; further checkouts block on a condition
/// variable until a client is returned. Connection setup failures
/// surface as `kIOError`.
class ICEBERG_HIVE_EXPORT HmsClientPool {
 public:
  /// \brief Build a pool sized by `hive.client-pool-size`. Validates the
  ///        HiveCatalogProperties (URI, transport) by opening one client
  ///        eagerly so configuration errors surface from `Make` rather
  ///        than the first checkout.
  static Result<std::unique_ptr<HmsClientPool>> Make(const HiveCatalogProperties& config);

  ~HmsClientPool();

  HmsClientPool(const HmsClientPool&) = delete;
  HmsClientPool& operator=(const HmsClientPool&) = delete;
  HmsClientPool(HmsClientPool&&) = delete;
  HmsClientPool& operator=(HmsClientPool&&) = delete;

  /// \brief Check out a client, invoke `fn(client)`, return the client
  ///        to the pool, and propagate `fn`'s result.
  ///
  /// `fn` must take a `HmsClient*` and return a `Result<T>` (any T,
  /// including `void` via `Status`). The client is held exclusively for
  /// the entire duration of `fn`, so callers can chain multiple RPCs on
  /// the same connection (e.g., `HiveTableOperations::Refresh` followed
  /// by `Commit`).
  ///
  /// Matches Java `ClientPoolImpl.run`'s reconnect-once contract: if
  /// `fn` returns an error tagged `kServiceUnavailable` (raised by
  /// `TransportError`, see `hive_errors.h`), the dead client is
  /// discarded, a fresh one is built via `HmsClient::Connect`, and `fn`
  /// is retried once. The second attempt's result -- success or any
  /// error, including another transport failure -- is returned verbatim
  /// so callers see at most one transparent reconnect per RPC sequence.
  template <typename F>
  auto Run(F&& fn) -> decltype(fn(std::declval<HmsClient*>())) {
    auto checkout = Checkout();
    if (!checkout.has_value()) {
      return std::unexpected(checkout.error());
    }
    auto client = std::move(*checkout);
    auto result = fn(client.get());
    if (!result.has_value() && result.error().kind == ErrorKind::kServiceUnavailable) {
      // Drop the broken connection without returning it to the idle queue.
      auto fresh = Reconnect(std::move(client));
      if (!fresh.has_value()) {
        return std::unexpected(fresh.error());
      }
      client = std::move(*fresh);
      result = fn(client.get());
      // Java `ClientPoolImpl.run` only retries once: if the fresh client
      // also reports a transport failure, do NOT re-enqueue it -- that
      // would let the next caller pick up a dead transport and start the
      // reconnect loop all over again. Checkin(nullptr) frees the slot.
      if (!result.has_value() && result.error().kind == ErrorKind::kServiceUnavailable) {
        Checkin(nullptr);
        return result;
      }
    }
    Checkin(std::move(client));
    return result;
  }

 private:
  HmsClientPool(HiveCatalogProperties config, std::size_t pool_size,
                std::unique_ptr<HmsClient> seed);

  /// \brief Block until a client is available, or build a fresh one if
  ///        we are below `pool_size_`. Returns `kIOError` if a freshly-
  ///        built client fails to connect.
  Result<std::unique_ptr<HmsClient>> Checkout();

  /// \brief Return a checked-out client to the idle queue.
  void Checkin(std::unique_ptr<HmsClient> client);

  /// \brief Discard `stale` and build a replacement via
  ///        `HmsClient::Connect`, keeping the `outstanding_` accounting
  ///        intact. On replacement-build failure the slot is freed and
  ///        the underlying error is returned so the caller can surface
  ///        it directly.
  Result<std::unique_ptr<HmsClient>> Reconnect(std::unique_ptr<HmsClient> stale);

  HiveCatalogProperties config_;
  std::size_t pool_size_;

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::unique_ptr<HmsClient>> idle_;
  std::size_t outstanding_ = 0;
};

}  // namespace iceberg::hive
