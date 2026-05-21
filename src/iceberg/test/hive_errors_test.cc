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

// Pin the contract of `CommitStateUnknownOnTransportFailure`: every
// mutating HiveCatalog op funnels Pool::Run's result through this
// helper so a `kServiceUnavailable` escape (pool reconnect-failure or
// post-reconnect retry-also-transport-failure) cannot invite the
// caller to retry on top of a possibly-landed first attempt. But the
// translation is gated on `mutation_attempted` -- a transport failure
// during a pre-read (GetTable / GetDatabase) leaves HMS state
// unchanged and must remain a retriable kServiceUnavailable.

TEST(CommitStateUnknownHelperTest, TranslatesServiceUnavailableAfterMutation) {
  Result<int> svc =
      ServiceUnavailable("HMS transport failed and reconnect also failed: nope");
  auto translated = CommitStateUnknownOnTransportFailure(
      std::move(svc), "TestOp::DoMutation", /*mutation_attempted=*/true);
  ASSERT_FALSE(translated.has_value());
  EXPECT_EQ(translated.error().kind, ErrorKind::kCommitStateUnknown);
  EXPECT_NE(translated.error().message.find("TestOp::DoMutation"), std::string::npos);
  EXPECT_NE(translated.error().message.find("nope"), std::string::npos);
}

TEST(CommitStateUnknownHelperTest, PreservesServiceUnavailableBeforeMutation) {
  // Pre-read transport failure: HMS state unmodified, caller can retry.
  // The helper must NOT convert this -- otherwise legitimate retries would
  // be turned into kCommitStateUnknown and stop iceberg::Transaction.
  Result<int> svc = ServiceUnavailable("GetTable transport failed");
  auto translated = CommitStateUnknownOnTransportFailure(
      std::move(svc), "TestOp::DropTable", /*mutation_attempted=*/false);
  ASSERT_FALSE(translated.has_value());
  EXPECT_EQ(translated.error().kind, ErrorKind::kServiceUnavailable)
      << "transport blips before the mutating RPC must remain retriable";
  EXPECT_EQ(translated.error().message, "GetTable transport failed");
}

TEST(CommitStateUnknownHelperTest, PreservesOtherErrors) {
  Result<int> al = AlreadyExists("warehouse already exists");
  auto translated = CommitStateUnknownOnTransportFailure(std::move(al), "TestOp::Create",
                                                         /*mutation_attempted=*/true);
  ASSERT_FALSE(translated.has_value());
  EXPECT_EQ(translated.error().kind, ErrorKind::kAlreadyExists)
      << "non-transport errors must pass through verbatim, otherwise we would "
         "blur a genuine first-attempt conflict into kCommitStateUnknown";
  EXPECT_EQ(translated.error().message, "warehouse already exists");
}

TEST(CommitStateUnknownHelperTest, PreservesAlreadyCommitStateUnknown) {
  Result<int> usk = CommitStateUnknown("inner recovery decided indeterminate");
  auto translated = CommitStateUnknownOnTransportFailure(std::move(usk), "TestOp::Commit",
                                                         /*mutation_attempted=*/true);
  ASSERT_FALSE(translated.has_value());
  EXPECT_EQ(translated.error().kind, ErrorKind::kCommitStateUnknown);
  EXPECT_EQ(translated.error().message, "inner recovery decided indeterminate")
      << "an inner-recovery CommitStateUnknown should not be re-wrapped";
}

TEST(CommitStateUnknownHelperTest, PreservesSuccess) {
  Result<int> ok = 42;
  auto translated = CommitStateUnknownOnTransportFailure(std::move(ok), "TestOp::Probe",
                                                         /*mutation_attempted=*/true);
  ASSERT_TRUE(translated.has_value());
  EXPECT_EQ(*translated, 42);
}

TEST(CommitStateUnknownHelperTest, WorksWithStatus) {
  Status svc = ServiceUnavailable("reconnect doubly-broken");
  auto translated = CommitStateUnknownOnTransportFailure(
      std::move(svc), "TestOp::DropThing", /*mutation_attempted=*/true);
  ASSERT_FALSE(translated.has_value());
  EXPECT_EQ(translated.error().kind, ErrorKind::kCommitStateUnknown);
  EXPECT_NE(translated.error().message.find("TestOp::DropThing"), std::string::npos);

  Status ok;  // success
  auto pass_through = CommitStateUnknownOnTransportFailure(
      std::move(ok), "TestOp::DropThing", /*mutation_attempted=*/true);
  EXPECT_TRUE(pass_through.has_value());

  Status pre_read = ServiceUnavailable("GetDatabase blew up");
  auto preserved = CommitStateUnknownOnTransportFailure(
      std::move(pre_read), "TestOp::DropThing", /*mutation_attempted=*/false);
  ASSERT_FALSE(preserved.has_value());
  EXPECT_EQ(preserved.error().kind, ErrorKind::kServiceUnavailable)
      << "Status overload must honour the mutation_attempted gate the same way";
}

}  // namespace iceberg::hive
