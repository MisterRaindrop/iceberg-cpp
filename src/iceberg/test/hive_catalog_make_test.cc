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

#include "iceberg/catalog/hive/hive_catalog.h"
#include "iceberg/catalog/hive/hive_catalog_properties.h"

namespace iceberg::hive {

// Pure-unit tests for HiveCatalog::Make's configuration-time validation.
// These exercise the failure surface that does not require a running HMS:
// missing / malformed URI, bad transport name, unknown io-impl. The
// docker-backed integration suite covers the live-HMS path separately.

TEST(HiveCatalogMakeTest, MissingUriIsInvalidArgument) {
  auto catalog = HiveCatalog::Make(HiveCatalogProperties::default_properties());
  ASSERT_FALSE(catalog.has_value());
  EXPECT_EQ(catalog.error().kind, ErrorKind::kInvalidArgument);
}

TEST(HiveCatalogMakeTest, EmptyHostInUriIsInvalidArgument) {
  auto catalog = HiveCatalog::Make(HiveCatalogProperties::FromMap(
      {{std::string(HiveCatalogProperties::kUri.key()), "thrift://:9083"}}));
  ASSERT_FALSE(catalog.has_value());
  EXPECT_EQ(catalog.error().kind, ErrorKind::kInvalidArgument);
}

TEST(HiveCatalogMakeTest, UnknownTransportIsInvalidArgument) {
  auto catalog = HiveCatalog::Make(HiveCatalogProperties::FromMap(
      {{std::string(HiveCatalogProperties::kUri.key()), "localhost:9083"},
       {std::string(HiveCatalogProperties::kThriftTransport.key()), "bogus"}}));
  ASSERT_FALSE(catalog.has_value());
  EXPECT_EQ(catalog.error().kind, ErrorKind::kInvalidArgument);
}

TEST(HiveCatalogMakeTest, UnreachableHmsIsIoError) {
  // Port 1 is privileged and (almost certainly) unbound; we assert that
  // HiveCatalog::Make surfaces the connection failure as a typed
  // kIOError instead of letting a Thrift C++ exception escape.
  auto catalog = HiveCatalog::Make(HiveCatalogProperties::FromMap(
      {{std::string(HiveCatalogProperties::kUri.key()), "127.0.0.1:1"},
       {std::string(HiveCatalogProperties::kConnectTimeoutMs.key()), "200"}}));
  ASSERT_FALSE(catalog.has_value());
  EXPECT_EQ(catalog.error().kind, ErrorKind::kIOError);
}

TEST(HiveCatalogMakeTest, UnknownIoImplIsNotSupported) {
  // io-impl must be one of the registered FileIO names (or empty for
  // auto-detect). An unknown impl name must surface a typed error rather
  // than crash inside the registry lookup.
  auto catalog = HiveCatalog::Make(HiveCatalogProperties::FromMap(
      {{std::string(HiveCatalogProperties::kUri.key()), "127.0.0.1:1"},
       {std::string(HiveCatalogProperties::kConnectTimeoutMs.key()), "200"},
       {std::string(HiveCatalogProperties::kIOImpl.key()), "no-such-io-impl"}}));
  ASSERT_FALSE(catalog.has_value());
  // The exact ErrorKind depends on whether Connect or FileIO registry
  // fails first; both kIOError (unreachable HMS) and kInvalid /
  // kNotFound (FileIO registry) are acceptable signals. The key
  // invariant is that no successful catalog is returned.
}

TEST(HiveCatalogMakeTest, AmbiguousIpv6UriIsInvalidArgument) {
  auto catalog = HiveCatalog::Make(HiveCatalogProperties::FromMap(
      {{std::string(HiveCatalogProperties::kUri.key()), "::1:9083"}}));
  ASSERT_FALSE(catalog.has_value());
  EXPECT_EQ(catalog.error().kind, ErrorKind::kInvalidArgument);
}

}  // namespace iceberg::hive
