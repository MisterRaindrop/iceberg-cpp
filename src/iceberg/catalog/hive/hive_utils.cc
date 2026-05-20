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

#include "iceberg/catalog/hive/hive_utils.h"

#include <array>
#include <cctype>
#include <format>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/util/macros.h"

namespace iceberg::hive {

namespace {

constexpr std::array<std::string_view, 4> kReservedKeys = {
    kCommentProperty, kLocationProperty, kOwnerProperty, kOwnerTypeProperty};

bool IsReservedKey(std::string_view key) {
  for (const auto& reserved : kReservedKeys) {
    if (key == reserved) {
      return true;
    }
  }
  return false;
}

std::string_view GetOr(const std::unordered_map<std::string, std::string>& properties,
                       std::string_view key, std::string_view fallback) {
  auto it = properties.find(std::string(key));
  if (it == properties.end()) {
    return fallback;
  }
  return it->second;
}

std::string TrimTrailingSlash(std::string_view path) {
  while (path.size() > 1 && path.back() == '/') {
    path.remove_suffix(1);
  }
  return std::string(path);
}

}  // namespace

Status ValidateNamespace(const Namespace& ns) {
  if (ns.levels.size() != 1) {
    return InvalidArgument(
        "Hive Metastore only supports single-level namespaces; got {} level(s).",
        ns.levels.size());
  }
  if (ns.levels[0].empty()) {
    return InvalidArgument("Hive namespace cannot have an empty name.");
  }
  return {};
}

Status ValidateOwnerSettings(
    const std::unordered_map<std::string, std::string>& properties) {
  const bool has_owner_type = properties.contains(std::string(kOwnerTypeProperty));
  const bool has_owner = properties.contains(std::string(kOwnerProperty));
  if (has_owner_type && !has_owner) {
    return InvalidArgument(
        "Hive namespace property 'owner-type' requires 'owner' to also be set.");
  }
  return {};
}

Result<HiveDatabase> ConvertToHiveDatabase(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  ICEBERG_RETURN_UNEXPECTED(ValidateOwnerSettings(properties));

  HiveDatabase database;
  database.name = ns.levels[0];
  database.description =
      std::string(GetOr(properties, kCommentProperty, std::string_view{}));
  database.location_uri =
      std::string(GetOr(properties, kLocationProperty, std::string_view{}));
  database.owner_name =
      std::string(GetOr(properties, kOwnerProperty, std::string_view{}));
  database.owner_type =
      std::string(GetOr(properties, kOwnerTypeProperty, std::string_view{}));

  for (const auto& [key, value] : properties) {
    if (!IsReservedKey(key)) {
      database.parameters.emplace(key, value);
    }
  }
  return database;
}

HiveNamespace ConvertFromHiveDatabase(const HiveDatabase& database) {
  HiveNamespace result;
  result.ns.levels = {database.name};
  result.properties = database.parameters;
  if (!database.description.empty()) {
    result.properties[std::string(kCommentProperty)] = database.description;
  }
  if (!database.location_uri.empty()) {
    result.properties[std::string(kLocationProperty)] = database.location_uri;
  }
  if (!database.owner_name.empty()) {
    result.properties[std::string(kOwnerProperty)] = database.owner_name;
  }
  if (!database.owner_type.empty()) {
    result.properties[std::string(kOwnerTypeProperty)] = database.owner_type;
  }
  return result;
}

Result<HiveTable> ConvertToHiveTable(
    const TableIdentifier& identifier, const std::vector<HiveColumn>& columns,
    std::string_view metadata_location, std::string_view location,
    const std::unordered_map<std::string, std::string>& table_properties) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());

  HiveTable table;
  table.db_name = identifier.ns.levels[0];
  table.table_name = identifier.name;
  table.owner = std::string(GetOr(table_properties, kOwnerProperty, std::string_view{}));
  table.table_type = "EXTERNAL_TABLE";
  table.location = std::string(location);
  table.columns = columns;
  table.serde = std::string(kLazySimpleSerDe);
  table.input_format = std::string(kFileInputFormat);
  table.output_format = std::string(kFileOutputFormat);

  // Mandatory Iceberg-on-HMS marker parameters.
  table.parameters.emplace(std::string(kMetadataLocationKey), metadata_location);
  table.parameters.emplace(std::string(kTableTypeKey), std::string(kTableTypeIceberg));
  table.parameters.emplace(std::string(kExternalKey), std::string(kExternalTrue));

  // Forward any user-supplied table properties that aren't reserved.
  for (const auto& [key, value] : table_properties) {
    if (!IsReservedKey(key) && !table.parameters.contains(key)) {
      table.parameters.emplace(key, value);
    }
  }
  return table;
}

Result<std::string> GetMetadataLocation(
    const std::unordered_map<std::string, std::string>& table_parameters) {
  auto it = table_parameters.find(std::string(kMetadataLocationKey));
  if (it == table_parameters.end() || it->second.empty()) {
    return NotFound("HMS table is missing '{}' parameter; not an Iceberg table.",
                    kMetadataLocationKey);
  }
  return it->second;
}

namespace {
bool IEquals(std::string_view lhs, std::string_view rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (std::size_t i = 0; i < lhs.size(); ++i) {
    const auto a = static_cast<unsigned char>(lhs[i]);
    const auto b = static_cast<unsigned char>(rhs[i]);
    if (std::tolower(a) != std::tolower(b)) {
      return false;
    }
  }
  return true;
}
}  // namespace

Status ValidateIcebergTable(
    const TableIdentifier& identifier,
    const std::unordered_map<std::string, std::string>& table_parameters) {
  auto it = table_parameters.find(std::string(kTableTypeKey));
  if (it == table_parameters.end() || it->second.empty()) {
    return NoSuchTable(
        "HMS table {} has no '{}' parameter; refusing to treat it as an Iceberg table.",
        identifier.ToString(), kTableTypeKey);
  }
  if (!IEquals(it->second, kTableTypeIceberg)) {
    return NoSuchTable("HMS table {} has '{}={}'; expected '{}' (case-insensitive).",
                       identifier.ToString(), kTableTypeKey, it->second,
                       kTableTypeIceberg);
  }
  return {};
}

std::string GetDefaultTableLocation(std::string_view warehouse, const Namespace& ns,
                                    std::string_view table_name) {
  std::string base = TrimTrailingSlash(warehouse);
  std::string ns_part;
  for (std::size_t i = 0; i < ns.levels.size(); ++i) {
    if (i > 0) {
      ns_part += ".";
    }
    ns_part += ns.levels[i];
  }
  return std::format("{}/{}.db/{}", base, ns_part, table_name);
}

}  // namespace iceberg::hive
