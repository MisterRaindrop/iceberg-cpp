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

#include <string_view>

namespace iceberg::arrow {

/// \brief Reserved property keys for S3 FileIO configuration.
///
/// These properties are used to configure S3 access when creating an S3-backed FileIO
/// instance. They map to the corresponding Arrow S3Options fields.
struct S3Properties {
  static constexpr std::string_view kAccessKeyId = "s3.access-key-id";
  static constexpr std::string_view kSecretAccessKey = "s3.secret-access-key";
  static constexpr std::string_view kSessionToken = "s3.session-token";
  static constexpr std::string_view kRegion = "s3.region";
  static constexpr std::string_view kEndpoint = "s3.endpoint";
  static constexpr std::string_view kPathStyleAccess = "s3.path-style-access";
  static constexpr std::string_view kSslEnabled = "s3.ssl.enabled";
  static constexpr std::string_view kConnectTimeoutMs = "s3.connect-timeout-ms";
  static constexpr std::string_view kSocketTimeoutMs = "s3.socket-timeout-ms";
};

}  // namespace iceberg::arrow
