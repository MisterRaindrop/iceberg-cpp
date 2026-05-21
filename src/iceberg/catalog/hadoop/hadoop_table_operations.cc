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

#include "iceberg/catalog/hadoop/hadoop_table_operations.h"

#include <string>
#include <string_view>
#include <utility>

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/util/macros.h"

namespace iceberg::hadoop {

namespace {

// Trim leading and trailing ASCII whitespace from a string in-place.
void TrimAsciiWhitespace(std::string& s) {
  auto is_space = [](unsigned char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
  };
  while (!s.empty() && is_space(static_cast<unsigned char>(s.back()))) {
    s.pop_back();
  }
  size_t front = 0;
  while (front < s.size() && is_space(static_cast<unsigned char>(s[front]))) {
    ++front;
  }
  if (front > 0) {
    s.erase(0, front);
  }
}

Result<int64_t> ParseDecimalInt64(std::string_view text, std::string_view source) {
  if (text.empty()) {
    return Invalid("Empty version segment in {}.", source);
  }
  int64_t value = 0;
  for (char c : text) {
    if (c < '0' || c > '9') {
      return Invalid("Non-numeric character '{}' in {} version '{}'", c, source, text);
    }
    value = (value * 10) + (c - '0');
  }
  return value;
}

}  // namespace

Result<int64_t> ReadVersionHint(FileIO& file_io, std::string_view table_dir) {
  const std::string hint_path = VersionHintPath(table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto content,
                          file_io.ReadFile(hint_path, /*length=*/std::nullopt));
  TrimAsciiWhitespace(content);
  ICEBERG_ASSIGN_OR_RAISE(auto version, ParseDecimalInt64(content, "version-hint.text"));
  return version;
}

Result<int64_t> FindLatestMetadataVersion(FileIO& file_io, std::string_view table_dir) {
  const std::string metadata_dir = MetadataDir(table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto entries, file_io.ListDir(metadata_dir));
  int64_t best = -1;
  for (const auto& entry : entries) {
    if (entry.is_directory) {
      continue;
    }
    std::string_view name = entry.location;
    auto slash = name.find_last_of('/');
    if (slash != std::string_view::npos) {
      name.remove_prefix(slash + 1);
    }
    auto parsed = ParseMetadataFileName(name);
    if (!parsed.has_value()) {
      continue;
    }
    if (parsed->version > best) {
      best = parsed->version;
    }
  }
  if (best < 0) {
    return NoSuchTable("No metadata files found under '{}'.", metadata_dir);
  }
  return best;
}

Result<ResolvedMetadataPointer> ResolveCurrentMetadata(FileIO& file_io,
                                                       std::string_view table_dir) {
  ResolvedMetadataPointer pointer;

  auto hint = ReadVersionHint(file_io, table_dir);
  if (hint.has_value()) {
    pointer.version = *hint;
  } else {
    // Hint missing or corrupt: scan the metadata directory.
    ICEBERG_ASSIGN_OR_RAISE(auto scanned, FindLatestMetadataVersion(file_io, table_dir));
    pointer.version = scanned;
    pointer.from_listdir_fallback = true;
  }

  // The hint gives us the version; the file extension is decided by whichever
  // codec the writer chose. Probe the three accepted suffixes in turn.
  const std::string metadata_dir = MetadataDir(table_dir);
  for (auto codec : {MetadataCompressionCodec::kNone, MetadataCompressionCodec::kGzip,
                     MetadataCompressionCodec::kZstd}) {
    std::string candidate = metadata_dir + "/" + MetadataFileName(pointer.version, codec);
    ICEBERG_ASSIGN_OR_RAISE(auto exists, file_io.Exists(candidate));
    if (exists) {
      pointer.location = std::move(candidate);
      return pointer;
    }
  }
  return NoSuchTable("Metadata file v{}.metadata.json[.gz|.zstd] not found under '{}'.",
                     pointer.version, metadata_dir);
}

HadoopTableOperations::HadoopTableOperations(std::shared_ptr<FileIO> file_io,
                                             std::string table_dir)
    : file_io_(std::move(file_io)), table_dir_(std::move(table_dir)) {}

Result<std::shared_ptr<TableMetadata>> HadoopTableOperations::Refresh() {
  if (file_io_ == nullptr) {
    return InvalidArgument("HadoopTableOperations: FileIO is null.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto pointer, ResolveCurrentMetadata(*file_io_, table_dir_));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata,
                          TableMetadataUtil::Read(*file_io_, pointer.location));
  current_location_ = pointer.location;
  current_version_ = pointer.version;
  return std::shared_ptr<TableMetadata>(std::move(metadata));
}

}  // namespace iceberg::hadoop
