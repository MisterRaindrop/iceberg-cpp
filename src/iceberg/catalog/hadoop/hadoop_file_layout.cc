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

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"

#include <format>
#include <string>

#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg::hadoop {

namespace {

constexpr std::string_view kJsonSuffix = ".metadata.json";
constexpr std::string_view kVersionPrefix = "v";

std::string JoinUnderRoot(std::string_view root, std::string_view child) {
  std::string base{LocationUtil::StripTrailingSlash(root)};
  base.push_back('/');
  base.append(child);
  return base;
}

}  // namespace

bool IsS3Scheme(std::string_view location) {
  return location.starts_with("s3://") || location.starts_with("s3a://") ||
         location.starts_with("s3n://");
}

std::string_view Basename(std::string_view path) {
  auto slash = path.find_last_of('/');
  return slash == std::string_view::npos ? path : path.substr(slash + 1);
}

Result<MetadataCompressionCodec> ParseMetadataCompressionCodec(std::string_view name) {
  const std::string lower = StringUtils::ToLower(name);
  if (lower.empty() || lower == "none") {
    return MetadataCompressionCodec::kNone;
  }
  if (lower == "gzip") {
    return MetadataCompressionCodec::kGzip;
  }
  if (lower == "zstd") {
    return MetadataCompressionCodec::kZstd;
  }
  return InvalidArgument(
      "Unknown metadata compression codec '{}'; expected 'none', 'gzip', or 'zstd'.",
      name);
}

std::string_view MetadataCompressionCodecName(MetadataCompressionCodec codec) {
  switch (codec) {
    case MetadataCompressionCodec::kNone:
      return "none";
    case MetadataCompressionCodec::kGzip:
      return "gzip";
    case MetadataCompressionCodec::kZstd:
      return "zstd";
  }
  return "none";
}

Status ValidateNamespaceLevel(std::string_view level) {
  if (level.empty()) {
    return InvalidArgument("Namespace level must not be empty.");
  }
  if (level.find('/') != std::string_view::npos) {
    return InvalidArgument("Namespace level '{}' must not contain '/'.", level);
  }
  return {};
}

Status ValidateNamespace(const Namespace& ns) {
  for (const auto& level : ns.levels) {
    ICEBERG_RETURN_UNEXPECTED(ValidateNamespaceLevel(level));
  }
  return {};
}

Status ValidateTableIdentifier(const TableIdentifier& identifier) {
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());
  if (identifier.name.find('/') != std::string::npos) {
    return InvalidArgument("Table name '{}' must not contain '/'.", identifier.name);
  }
  return ValidateNamespace(identifier.ns);
}

Result<std::string> NamespaceDir(std::string_view warehouse, const Namespace& ns) {
  if (warehouse.empty()) {
    return InvalidArgument("warehouse must not be empty when computing namespace dir.");
  }
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));

  std::string out{LocationUtil::StripTrailingSlash(warehouse)};
  for (const auto& level : ns.levels) {
    out.push_back('/');
    out.append(level);
  }
  return out;
}

Result<std::string> TableDir(std::string_view warehouse,
                             const TableIdentifier& identifier) {
  ICEBERG_RETURN_UNEXPECTED(ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, NamespaceDir(warehouse, identifier.ns));
  return JoinUnderRoot(ns_dir, identifier.name);
}

std::string MetadataDir(std::string_view table_dir) {
  return JoinUnderRoot(table_dir, "metadata");
}

std::string DataDir(std::string_view table_dir) {
  return JoinUnderRoot(table_dir, "data");
}

std::string MetadataFileName(int64_t version, MetadataCompressionCodec codec) {
  switch (codec) {
    case MetadataCompressionCodec::kNone:
      return std::format("v{}.metadata.json", version);
    case MetadataCompressionCodec::kGzip:
      return std::format("v{}.metadata.json.gz", version);
    case MetadataCompressionCodec::kZstd:
      return std::format("v{}.metadata.json.zstd", version);
  }
  return std::format("v{}.metadata.json", version);
}

std::string MetadataFilePath(std::string_view table_dir, int64_t version,
                             MetadataCompressionCodec codec) {
  return JoinUnderRoot(MetadataDir(table_dir), MetadataFileName(version, codec));
}

std::string VersionHintPath(std::string_view table_dir) {
  return JoinUnderRoot(MetadataDir(table_dir), "version-hint.text");
}

std::string LockFilePath(std::string_view table_dir) {
  return JoinUnderRoot(MetadataDir(table_dir), "_lock");
}

Result<bool> IsHadoopTableDir(FileIO& file_io, std::string_view dir_location) {
  // A HadoopCatalog table contains a metadata/ subdirectory with at least one
  // v{N}.metadata.json[.codec] file. We probe the metadata dir first and only
  // list its contents if it exists, to avoid expensive listings on plain
  // namespace directories.
  const std::string metadata_dir = MetadataDir(dir_location);
  ICEBERG_ASSIGN_OR_RAISE(auto exists, file_io.Exists(metadata_dir));
  if (!exists) {
    return false;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto is_dir, file_io.IsDirectory(metadata_dir));
  if (!is_dir) {
    return false;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto entries, file_io.ListDir(metadata_dir));
  for (const auto& entry : entries) {
    if (entry.is_directory) {
      continue;
    }
    if (ParseMetadataFileName(Basename(entry.location)).has_value()) {
      return true;
    }
  }
  return false;
}

Result<MetadataFileRef> ParseMetadataFileName(std::string_view file_name) {
  if (!file_name.starts_with(kVersionPrefix)) {
    return InvalidArgument(
        "Metadata filename '{}' does not start with 'v' (expected vN.metadata.json).",
        file_name);
  }

  // Strip trailing codec suffix.
  std::string_view stripped = file_name;
  MetadataCompressionCodec codec = MetadataCompressionCodec::kNone;
  if (stripped.ends_with(".gz")) {
    codec = MetadataCompressionCodec::kGzip;
    stripped.remove_suffix(3);
  } else if (stripped.ends_with(".zstd")) {
    codec = MetadataCompressionCodec::kZstd;
    stripped.remove_suffix(5);
  }
  if (!stripped.ends_with(kJsonSuffix)) {
    return InvalidArgument("Metadata filename '{}' does not end with '.metadata.json'.",
                           file_name);
  }
  stripped.remove_suffix(kJsonSuffix.size());

  // Remaining piece must be `v<digits>`.
  std::string_view version_str = stripped;
  version_str.remove_prefix(kVersionPrefix.size());
  if (version_str.empty()) {
    return InvalidArgument("Metadata filename '{}' has empty version segment.",
                           file_name);
  }
  auto version = StringUtils::ParseNumber<int64_t>(version_str);
  if (!version.has_value()) {
    return InvalidArgument("Metadata filename '{}' has non-numeric version segment '{}'.",
                           file_name, version_str);
  }
  return MetadataFileRef{.version = *version, .codec = codec};
}

}  // namespace iceberg::hadoop
