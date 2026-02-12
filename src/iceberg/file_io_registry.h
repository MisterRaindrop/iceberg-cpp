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

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "iceberg/file_io.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Reserved property keys for FileIO configuration.
struct FileIOProperties {
  /// \brief The "io-impl" property key used to specify a custom FileIO implementation.
  static constexpr std::string_view kImpl = "io-impl";
};

/// \brief A registry for FileIO factory functions.
///
/// Allows users to register and load FileIO implementations by name. This enables
/// pluggable FileIO support for different storage backends (S3, local, custom, etc.).
///
/// Usage:
/// \code
/// // Register a custom FileIO implementation
/// FileIORegistry::Register("com.mycompany.MyFileIO",
///     [](const std::string& warehouse, const auto& props) {
///         return std::make_shared<MyFileIO>(warehouse, props);
///     });
///
/// // Load a registered FileIO
/// auto file_io = FileIORegistry::Load("com.mycompany.MyFileIO", warehouse, properties);
/// \endcode
class ICEBERG_EXPORT FileIORegistry {
 public:
  /// \brief Well-known FileIO implementation names.
  static constexpr std::string_view kArrowS3FileIO = "org.apache.iceberg.arrow.S3FileIO";
  static constexpr std::string_view kArrowLocalFileIO =
      "org.apache.iceberg.arrow.LocalFileIO";

  /// \brief Factory function type for creating FileIO instances.
  using Factory = std::function<Result<std::shared_ptr<FileIO>>(
      const std::string& warehouse,
      const std::unordered_map<std::string, std::string>& properties)>;

  /// \brief Register a FileIO factory function under the given name.
  ///
  /// \param name The name to register the factory under.
  /// \param factory The factory function.
  static void Register(const std::string& name, Factory factory);

  /// \brief Load a FileIO instance by name.
  ///
  /// \param name The registered name of the FileIO implementation.
  /// \param warehouse The warehouse location (URI or path).
  /// \param properties Configuration properties.
  /// \return A shared_ptr to the created FileIO, or an error if not found.
  static Result<std::shared_ptr<FileIO>> Load(
      const std::string& name, const std::string& warehouse,
      const std::unordered_map<std::string, std::string>& properties);
};

}  // namespace iceberg
