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
