// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "iceberg/arrow/arrow_io_register.h"

#include <mutex>
#include <string>

#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/file_io_registry.h"

#if ICEBERG_HDFS_ENABLED
#  include <arrow/filesystem/hdfs.h>
#  include <arrow/io/hdfs.h>
#  include <arrow/result.h>

#  include "iceberg/arrow/arrow_io_internal.h"
#endif

namespace iceberg::arrow {

namespace {

void RegisterLocalFileIO() {
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowLocalFileIO),
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return MakeLocalFileIO(); });
}

void RegisterS3FileIO() {
#if ICEBERG_S3_ENABLED
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowS3FileIO),
      [](const std::unordered_map<std::string, std::string>& properties)
          -> Result<std::unique_ptr<FileIO>> { return MakeS3FileIO(properties); });
#endif
}

void RegisterHdfsFileIO() {
#if ICEBERG_HDFS_ENABLED
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowHdfsFileIO),
      [](const std::unordered_map<std::string, std::string>& properties)
          -> Result<std::unique_ptr<FileIO>> {
        ::arrow::fs::HdfsOptions options;
        // Pass through Hadoop config keys verbatim so users can set
        // dfs.nameservices, dfs.client.failover.proxy.provider.*, etc. as
        // they would on the JVM. fs.defaultFS is recognised explicitly.
        auto fs_default = properties.find("fs.defaultFS");
        if (fs_default != properties.end() && !fs_default->second.empty()) {
          auto parsed = ::arrow::fs::HdfsOptions::FromUri(fs_default->second);
          if (parsed.ok()) {
            options = *parsed;
          }
        }
        for (const auto& [key, value] : properties) {
          if (key.starts_with("hadoop.") || key.starts_with("dfs.") ||
              key.starts_with("fs.")) {
            // arrow exposes extra_conf on the HdfsConnectionConfig nested
            // inside HdfsOptions, not on the options struct itself; see
            // arrow/io/hdfs.h:87.
            options.connection_config.extra_conf.emplace(key, value);
          }
        }
        auto fs_result = ::arrow::fs::HadoopFileSystem::Make(options);
        if (!fs_result.ok()) {
          return IOError("HadoopFileSystem::Make failed: {}",
                         fs_result.status().ToString());
        }
        return std::make_unique<ArrowFileSystemFileIO>(*fs_result);
      });
#endif
}

}  // namespace

void EnsureArrowFileIOsRegistered() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    RegisterLocalFileIO();
    RegisterS3FileIO();
    RegisterHdfsFileIO();
  });
}

[[maybe_unused]] const bool kArrowFileIOsRegistered = []() {
  EnsureArrowFileIOsRegistered();
  return true;
}();

}  // namespace iceberg::arrow
