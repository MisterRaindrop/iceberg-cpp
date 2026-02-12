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

#include <gtest/gtest.h>

#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

TEST(FileIORegistryTest, LoadUnregisteredReturnsNotFound) {
  auto result = FileIORegistry::Load("com.nonexistent.FileIO", "/warehouse", {});
  EXPECT_THAT(result, IsError(ErrorKind::kNotFound));
  EXPECT_THAT(result, HasErrorMessage("FileIO implementation not found"));
}

TEST(FileIORegistryTest, RegisterAndLoad) {
  FileIORegistry::Register(
      "com.test.TestFileIO",
      [](const std::string& /*warehouse*/,
         const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::shared_ptr<FileIO>> {
        return std::make_shared<FileIO>();
      });

  auto result = FileIORegistry::Load("com.test.TestFileIO", "/warehouse", {});
  ASSERT_THAT(result, IsOk());
  EXPECT_NE(result.value(), nullptr);
}

TEST(FileIORegistryTest, RegisterOverwritesPrevious) {
  int call_count = 0;
  FileIORegistry::Register(
      "com.test.OverwriteFileIO",
      [&call_count](const std::string& /*warehouse*/,
                    const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::shared_ptr<FileIO>> {
        call_count = 1;
        return std::make_shared<FileIO>();
      });

  FileIORegistry::Register(
      "com.test.OverwriteFileIO",
      [&call_count](const std::string& /*warehouse*/,
                    const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::shared_ptr<FileIO>> {
        call_count = 2;
        return std::make_shared<FileIO>();
      });

  auto result = FileIORegistry::Load("com.test.OverwriteFileIO", "/warehouse", {});
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(call_count, 2);
}

}  // namespace iceberg
