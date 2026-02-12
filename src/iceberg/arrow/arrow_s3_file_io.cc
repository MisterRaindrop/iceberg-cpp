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

#include <cstdlib>
#include <mutex>
#include <string_view>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#ifdef ICEBERG_HAVE_S3
#include <arrow/filesystem/s3fs.h>
#endif

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/s3_properties.h"
#include "iceberg/util/macros.h"

namespace iceberg::arrow {

namespace {

bool IsS3Uri(std::string_view uri) { return uri.starts_with("s3://"); }

Status EnsureS3Initialized() {
#ifdef ICEBERG_HAVE_S3
  static std::once_flag init_flag;
  static ::arrow::Status init_status = ::arrow::Status::OK();
  std::call_once(init_flag, []() {
    ::arrow::fs::S3GlobalOptions options;
    init_status = ::arrow::fs::InitializeS3(options);
    if (init_status.ok()) {
      std::atexit([]() { (void)::arrow::fs::FinalizeS3(); });
    }
  });
  if (!init_status.ok()) {
    return std::unexpected<Error>{
        {.kind = ::iceberg::arrow::ToErrorKind(init_status),
         .message = init_status.ToString()}};
  }
  return {};
#else
  return NotImplemented("Arrow S3 support is not enabled");
#endif
}

#ifdef ICEBERG_HAVE_S3
/// \brief Configure S3Options from a properties map.
///
/// \param properties The configuration properties map.
/// \return Configured S3Options.
::arrow::fs::S3Options ConfigureS3Options(
    const std::unordered_map<std::string, std::string>& properties) {
  ::arrow::fs::S3Options options;

  // Configure credentials
  auto access_key_it = properties.find(S3Properties::kAccessKeyId);
  auto secret_key_it = properties.find(S3Properties::kSecretAccessKey);
  auto session_token_it = properties.find(S3Properties::kSessionToken);

  if (access_key_it != properties.end() && secret_key_it != properties.end()) {
    if (session_token_it != properties.end()) {
      options.ConfigureAccessKey(access_key_it->second, secret_key_it->second,
                                 session_token_it->second);
    } else {
      options.ConfigureAccessKey(access_key_it->second, secret_key_it->second);
    }
  } else {
    // Use default credential chain (environment, instance profile, etc.)
    options.ConfigureDefaultCredentials();
  }

  // Configure region
  auto region_it = properties.find(S3Properties::kRegion);
  if (region_it != properties.end()) {
    options.region = region_it->second;
  }

  // Configure endpoint (for MinIO, LocalStack, etc.)
  auto endpoint_it = properties.find(S3Properties::kEndpoint);
  if (endpoint_it != properties.end()) {
    options.endpoint_override = endpoint_it->second;
  }

  // Configure path-style access (needed for MinIO)
  auto path_style_it = properties.find(S3Properties::kPathStyleAccess);
  if (path_style_it != properties.end() && path_style_it->second == "true") {
    options.force_virtual_addressing = false;
  }

  // Configure SSL
  auto ssl_it = properties.find(S3Properties::kSslEnabled);
  if (ssl_it != properties.end() && ssl_it->second == "false") {
    options.scheme = "http";
  }

  // Configure timeouts
  auto connect_timeout_it = properties.find(S3Properties::kConnectTimeoutMs);
  if (connect_timeout_it != properties.end()) {
    double timeout_ms = std::stod(connect_timeout_it->second);
    if (timeout_ms >= 0) {
      options.connect_timeout = timeout_ms / 1000.0;
    }
  }

  auto socket_timeout_it = properties.find(S3Properties::kSocketTimeoutMs);
  if (socket_timeout_it != properties.end()) {
    double timeout_ms = std::stod(socket_timeout_it->second);
    if (timeout_ms >= 0) {
      options.request_timeout = timeout_ms / 1000.0;
    }
  }

  return options;
}

/// \brief Create an S3 FileSystem with the given options.
///
/// \param options The S3Options to use.
/// \return A shared_ptr to the S3FileSystem, or an error.
Result<std::shared_ptr<::arrow::fs::FileSystem>> MakeS3FileSystem(
    const ::arrow::fs::S3Options& options) {
  ICEBERG_RETURN_UNEXPECTED(EnsureS3Initialized());
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto fs, ::arrow::fs::S3FileSystem::Make(options));
  return fs;
}
#endif

}  // namespace

Result<std::unique_ptr<FileIO>> MakeS3FileIO(const std::string& uri) {
  if (!IsS3Uri(uri)) {
    return InvalidArgument("S3 URI must start with s3://");
  }
#ifndef ICEBERG_HAVE_S3
  return NotImplemented("Arrow S3 support is not enabled");
#else
  ICEBERG_RETURN_UNEXPECTED(EnsureS3Initialized());
  std::string path;
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto fs, ::arrow::fs::FileSystemFromUri(uri, &path));
  return std::make_unique<ArrowFileSystemFileIO>(std::move(fs));
#endif
}

Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    const std::string& uri,
    const std::unordered_map<std::string, std::string>& properties) {
  if (!IsS3Uri(uri)) {
    return InvalidArgument("S3 URI must start with s3://");
  }
#ifndef ICEBERG_HAVE_S3
  return NotImplemented("Arrow S3 support is not enabled");
#else
  // If properties are empty, use the simple URI-based resolution
  if (properties.empty()) {
    return MakeS3FileIO(uri);
  }

  // Create S3FileSystem with explicit configuration
  auto options = ConfigureS3Options(properties);
  ICEBERG_ASSIGN_OR_RAISE(auto fs, MakeS3FileSystem(options));

  // Return ArrowFileSystemFileIO with the configured S3 filesystem
  return std::make_unique<ArrowFileSystemFileIO>(std::move(fs));
#endif
}

}  // namespace iceberg::arrow
