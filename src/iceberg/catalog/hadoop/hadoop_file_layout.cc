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

#include <algorithm>
#include <cctype>
#include <format>
#include <string>

#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"

namespace iceberg::hadoop {

namespace {

constexpr std::string_view kJsonSuffix = ".metadata.json";
constexpr std::string_view kVersionPrefix = "v";

std::string ToLowerAscii(std::string_view s) {
  std::string out(s);
  std::ranges::transform(out, out.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return out;
}

std::string JoinUnderRoot(std::string_view root, std::string_view child) {
  std::string base{LocationUtil::StripTrailingSlash(root)};
  base.push_back('/');
  base.append(child);
  return base;
}

}  // namespace

Result<MetadataCompressionCodec> ParseMetadataCompressionCodec(std::string_view name) {
  const std::string lower = ToLowerAscii(name);
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

std::string NamespacePropertiesPath(std::string_view ns_dir) {
  return JoinUnderRoot(ns_dir, "namespace.properties");
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
  int64_t version = 0;
  for (char c : version_str) {
    if (c < '0' || c > '9') {
      return InvalidArgument(
          "Metadata filename '{}' has non-numeric version segment '{}'.", file_name,
          version_str);
    }
    version = (version * 10) + (c - '0');
  }
  return MetadataFileRef{.version = version, .codec = codec};
}

}  // namespace iceberg::hadoop
