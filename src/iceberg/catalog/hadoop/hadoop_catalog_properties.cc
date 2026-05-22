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

#include "iceberg/catalog/hadoop/hadoop_catalog_properties.h"

#include <utility>

#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg::hadoop {

HadoopCatalogProperties HadoopCatalogProperties::default_properties() { return {}; }

HadoopCatalogProperties HadoopCatalogProperties::FromMap(
    std::unordered_map<std::string, std::string> properties) {
  HadoopCatalogProperties config;
  config.configs_ = std::move(properties);
  return config;
}

Result<std::string_view> HadoopCatalogProperties::Warehouse() const {
  auto it = configs_.find(kWarehouse.key());
  if (it == configs_.end() || it->second.empty()) {
    return InvalidArgument(
        "Hadoop catalog configuration property 'warehouse' is required.");
  }
  return it->second;
}

Status HadoopCatalogProperties::Validate() const {
  // Read+parse raw string values directly instead of going through
  // ConfigBase::Get(): the latter calls ICEBERG_ASSIGN_OR_THROW for parse
  // failures, which would surface as a C++ exception out of Make().
  // Validate() is the right place to convert a malformed value into a
  // kInvalidArgument result.
  auto parse_int64 = [this](const auto& entry) -> Result<int64_t> {
    auto it = configs_.find(entry.key());
    if (it == configs_.end()) {
      return entry.value();
    }
    auto parsed = StringUtils::ParseNumber<int64_t>(it->second);
    if (!parsed.has_value()) {
      return InvalidArgument(
          "Hadoop catalog property '{}' must be an integer (got '{}': {}).", entry.key(),
          it->second, parsed.error().message);
    }
    return *parsed;
  };
  auto check_positive_ms = [&parse_int64](const auto& entry) -> Status {
    ICEBERG_ASSIGN_OR_RAISE(auto value, parse_int64(entry));
    if (value <= 0) {
      return InvalidArgument("Hadoop catalog property '{}' must be positive (got {}).",
                             entry.key(), value);
    }
    return {};
  };

  ICEBERG_RETURN_UNEXPECTED(check_positive_ms(kLockAcquireIntervalMs));
  ICEBERG_RETURN_UNEXPECTED(check_positive_ms(kLockAcquireTimeoutMs));
  ICEBERG_RETURN_UNEXPECTED(check_positive_ms(kLockHeartbeatIntervalMs));
  ICEBERG_RETURN_UNEXPECTED(check_positive_ms(kLockHeartbeatTimeoutMs));
  ICEBERG_RETURN_UNEXPECTED(check_positive_ms(kCacheExpirationIntervalMs));

  // heartbeat-interval should be strictly less than heartbeat-timeout, or
  // every heartbeat would be too late to refresh the lock. Equal values
  // theoretically work but leave zero margin for scheduler jitter; warn
  // by treating them as invalid so operators notice early.
  ICEBERG_ASSIGN_OR_RAISE(const auto interval, parse_int64(kLockHeartbeatIntervalMs));
  ICEBERG_ASSIGN_OR_RAISE(const auto timeout, parse_int64(kLockHeartbeatTimeoutMs));
  if (interval >= timeout) {
    return InvalidArgument(
        "Hadoop catalog property '{}' ({}ms) must be strictly less than '{}' ({}ms) "
        "or the heartbeat cannot refresh the lock in time.",
        kLockHeartbeatIntervalMs.key(), interval, kLockHeartbeatTimeoutMs.key(), timeout);
  }

  // lock-impl must be one of the recognised names; deferring to MakeLockManager
  // catches this too, but checking early keeps the failure attached to the
  // bad config rather than the first acquire. We read the raw string here
  // too, but lock-impl is a string entry so Get() is throw-safe in this
  // particular case -- using configs_ directly keeps Validate uniform.
  auto impl_it = configs_.find(kLockImpl.key());
  if (impl_it != configs_.end()) {
    const auto& impl = impl_it->second;
    if (!impl.empty() && impl != "in-memory" && impl != "file") {
      return InvalidArgument(
          "Hadoop catalog property '{}' must be 'in-memory' or 'file' (got '{}').",
          kLockImpl.key(), impl);
    }
  }
  return {};
}

std::unordered_map<std::string, std::string> HadoopCatalogProperties::ExtractHadoopConf()
    const {
  std::unordered_map<std::string, std::string> result;
  for (const auto& [key, value] : configs_) {
    if (key.starts_with(kHadoopPrefix) || key.starts_with(kDfsPrefix) ||
        key.starts_with(kFsPrefix)) {
      result.emplace(key, value);
    }
  }
  return result;
}

}  // namespace iceberg::hadoop
