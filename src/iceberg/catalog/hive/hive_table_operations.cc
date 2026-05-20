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

#include "iceberg/catalog/hive/hive_table_operations.h"

#include <optional>
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/hive/hive_utils.h"
#include "iceberg/catalog/hive/hms_client.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg::hive {

HiveTableOperations::HiveTableOperations(HmsClient* client,
                                         std::shared_ptr<FileIO> file_io,
                                         TableIdentifier identifier)
    : client_(client), file_io_(std::move(file_io)), identifier_(std::move(identifier)) {}

Result<HiveTableMetadataSnapshot> HiveTableOperations::Refresh() {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier_.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier_.Validate());

  ICEBERG_ASSIGN_OR_RAISE(auto hive_table,
                          client_->GetTable(identifier_.ns.levels[0], identifier_.name));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_location,
                          GetMetadataLocation(hive_table.parameters));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_json,
                          file_io_->ReadFile(metadata_location, /*length=*/std::nullopt));

  nlohmann::json metadata_obj;
  try {
    metadata_obj = nlohmann::json::parse(metadata_json);
  } catch (const nlohmann::json::parse_error& e) {
    return JsonParseError("Failed to parse metadata at '{}': {}", metadata_location,
                          e.what());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, TableMetadataFromJson(metadata_obj));

  return HiveTableMetadataSnapshot{
      .metadata = std::shared_ptr<TableMetadata>(metadata.release()),
      .metadata_location = std::move(metadata_location),
  };
}

}  // namespace iceberg::hive
