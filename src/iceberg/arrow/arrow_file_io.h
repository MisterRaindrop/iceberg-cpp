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

#include <memory>
#include <string>
#include <unordered_map>

#include "iceberg/file_io.h"
#include "iceberg/iceberg_bundle_export.h"
#include "iceberg/result.h"

namespace iceberg::arrow {

ICEBERG_BUNDLE_EXPORT std::unique_ptr<FileIO> MakeMockFileIO();

ICEBERG_BUNDLE_EXPORT std::unique_ptr<FileIO> MakeLocalFileIO();

/// \brief Create an S3 FileIO backed by Arrow's S3FileSystem.
///
/// This function initializes the S3 subsystem if not already initialized (thread-safe).
/// The S3 initialization is done once per process using std::call_once.
///
/// \param uri An S3 URI (must start with "s3://") used to validate the scheme.
/// \return A FileIO instance for S3 operations, or an error if S3 is not supported
///         or the URI is invalid.
ICEBERG_BUNDLE_EXPORT Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    const std::string& uri);

/// \brief Create an S3 FileIO with explicit configuration properties.
///
/// This overload allows passing S3-specific configuration such as credentials,
/// region, endpoint, and other settings. See S3Properties for available options.
///
/// The FileIO instance created by this function is thread-safe and can be shared
/// across multiple threads for concurrent catalog operations.
///
/// \param uri An S3 URI (must start with "s3://") used to validate the scheme.
/// \param properties Configuration properties for S3 access. See S3Properties for keys.
/// \return A FileIO instance for S3 operations, or an error if configuration fails.
ICEBERG_BUNDLE_EXPORT Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    const std::string& uri,
    const std::unordered_map<std::string, std::string>& properties);

}  // namespace iceberg::arrow
