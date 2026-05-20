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
  template <typename F>
  auto Run(F&& fn) -> decltype(fn(std::declval<HmsClient*>())) {
    auto checkout = Checkout();
    if (!checkout.has_value()) {
      return std::unexpected(checkout.error());
    }
    auto client = std::move(*checkout);
    auto result = fn(client.get());
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

  HiveCatalogProperties config_;
  std::size_t pool_size_;

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::unique_ptr<HmsClient>> idle_;
  std::size_t outstanding_ = 0;
};

}  // namespace iceberg::hive
