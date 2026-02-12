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

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/file_io_registry.h"

namespace iceberg::arrow {

namespace {

struct ArrowFileIORegistrar {
  ArrowFileIORegistrar() {
    FileIORegistry::Register(
        std::string(FileIORegistry::kArrowLocalFileIO),
        [](const std::string& /*warehouse*/,
           const std::unordered_map<std::string, std::string>& /*properties*/)
            -> Result<std::shared_ptr<FileIO>> { return MakeLocalFileIO(); });

    FileIORegistry::Register(
        std::string(FileIORegistry::kArrowS3FileIO),
        [](const std::string& warehouse,
           const std::unordered_map<std::string, std::string>& properties)
            -> Result<std::shared_ptr<FileIO>> {
          auto result = MakeS3FileIO(warehouse, properties);
          if (!result.has_value()) {
            return std::unexpected(result.error());
          }
          return std::shared_ptr<FileIO>(std::move(result.value()));
        });
  }
};

// Static initialization triggers registration at program startup
static ArrowFileIORegistrar registrar;

}  // namespace

}  // namespace iceberg::arrow
