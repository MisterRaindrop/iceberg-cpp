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

#include "iceberg/catalog/hive/hms_client_pool.h"

#include <utility>

#include "iceberg/util/macros.h"

namespace iceberg::hive {

namespace {
constexpr std::size_t kMinPoolSize = 1;
}  // namespace

HmsClientPool::HmsClientPool(HiveCatalogProperties config, std::size_t pool_size,
                             std::unique_ptr<HmsClient> seed)
    : config_(std::move(config)), pool_size_(pool_size) {
  if (seed) {
    idle_.push_back(std::move(seed));
  }
}

HmsClientPool::~HmsClientPool() = default;

Result<std::unique_ptr<HmsClientPool>> HmsClientPool::Make(
    const HiveCatalogProperties& config) {
  // Clamp the int value before casting so a negative configured size does
  // not wrap to SIZE_MAX (which would let the pool grow without bound on
  // every checkout).
  const int configured = config.Get(HiveCatalogProperties::kClientPoolSize);
  const std::size_t pool_size = configured < static_cast<int>(kMinPoolSize)
                                    ? kMinPoolSize
                                    : static_cast<std::size_t>(configured);
  // Open one client eagerly so configuration mistakes (bad URI, bad
  // transport, unreachable HMS) surface from `HiveCatalog::Make` rather
  // than the first catalog method call.
  ICEBERG_ASSIGN_OR_RAISE(auto seed, HmsClient::Connect(config));
  return std::unique_ptr<HmsClientPool>(
      new HmsClientPool(config, pool_size, std::move(seed)));
}

Result<std::unique_ptr<HmsClient>> HmsClientPool::Checkout() {
  std::unique_lock<std::mutex> lock(mu_);
  while (true) {
    if (!idle_.empty()) {
      auto client = std::move(idle_.front());
      idle_.pop_front();
      ++outstanding_;
      return client;
    }
    if (idle_.size() + outstanding_ < pool_size_) {
      // Grow the pool lazily: account for the new client before releasing
      // the lock so a concurrent Checkin keeps the bookkeeping straight,
      // then connect outside the lock to avoid serialising HMS handshakes.
      ++outstanding_;
      lock.unlock();
      auto fresh = HmsClient::Connect(config_);
      if (!fresh.has_value()) {
        lock.lock();
        --outstanding_;
        cv_.notify_one();
        return std::unexpected(fresh.error());
      }
      return std::move(*fresh);
    }
    cv_.wait(lock);
  }
}

void HmsClientPool::Checkin(std::unique_ptr<HmsClient> client) {
  std::lock_guard<std::mutex> lock(mu_);
  --outstanding_;
  if (client) {
    idle_.push_back(std::move(client));
  }
  cv_.notify_one();
}

}  // namespace iceberg::hive
