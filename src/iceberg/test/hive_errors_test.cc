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

#include "iceberg/catalog/hive/hive_errors.h"

#include <gtest/gtest.h>

namespace iceberg::hive {

TEST(HiveErrorsTest, NoSuchObjectMapsToNotFound) {
  const auto err = NoSuchObjectError("get_table", "table 'sales.orders' not found");
  EXPECT_EQ(err.error().kind, ErrorKind::kNotFound);
  EXPECT_NE(err.error().message.find("get_table"), std::string::npos);
  EXPECT_NE(err.error().message.find("sales.orders"), std::string::npos);
}

TEST(HiveErrorsTest, UnknownDbMapsToNoSuchNamespace) {
  const auto err = UnknownDBError("get_database", "no such db: warehouse");
  EXPECT_EQ(err.error().kind, ErrorKind::kNoSuchNamespace);
  EXPECT_NE(err.error().message.find("warehouse"), std::string::npos);
}

TEST(HiveErrorsTest, UnknownTableMapsToNoSuchTable) {
  const auto err = UnknownTableError("get_table", "no such table");
  EXPECT_EQ(err.error().kind, ErrorKind::kNoSuchTable);
}

TEST(HiveErrorsTest, AlreadyExistsMapsToAlreadyExists) {
  const auto err = AlreadyExistsError("create_database", "warehouse already exists");
  EXPECT_EQ(err.error().kind, ErrorKind::kAlreadyExists);
}

TEST(HiveErrorsTest, InvalidObjectMapsToInvalidArgument) {
  const auto err = InvalidObjectError("create_table", "bad column type");
  EXPECT_EQ(err.error().kind, ErrorKind::kInvalidArgument);
}

TEST(HiveErrorsTest, InvalidPartitionMapsToInvalidArgument) {
  const auto err = InvalidPartitionError("add_partition", "missing partition key");
  EXPECT_EQ(err.error().kind, ErrorKind::kInvalidArgument);
}

TEST(HiveErrorsTest, InvalidOperationMapsToNotAllowed) {
  const auto err = InvalidOperationError("drop_database", "namespace not empty");
  EXPECT_EQ(err.error().kind, ErrorKind::kNotAllowed);
}

TEST(HiveErrorsTest, ConfigValSecurityMapsToNotAuthorized) {
  const auto err = ConfigValSecurityError("alter_table", "permission denied");
  EXPECT_EQ(err.error().kind, ErrorKind::kNotAuthorized);
}

TEST(HiveErrorsTest, MetaExceptionMapsToIoError) {
  const auto err = MetaError("get_all_databases", "internal SQL failure");
  EXPECT_EQ(err.error().kind, ErrorKind::kIOError);
  EXPECT_NE(err.error().message.find("MetaException"), std::string::npos);
}

TEST(HiveErrorsTest, GenericThriftMapsToIoError) {
  const auto err = GenericThriftError("alter_table", "connection reset by peer");
  EXPECT_EQ(err.error().kind, ErrorKind::kIOError);
  EXPECT_NE(err.error().message.find("connection reset by peer"), std::string::npos);
}

TEST(HiveErrorsTest, ContextAndMessageBothAppearInOutput) {
  const auto err = NoSuchObjectError("xyz_call", "something_unique_xyz");
  EXPECT_NE(err.error().message.find("xyz_call"), std::string::npos);
  EXPECT_NE(err.error().message.find("something_unique_xyz"), std::string::npos);
}

}  // namespace iceberg::hive
