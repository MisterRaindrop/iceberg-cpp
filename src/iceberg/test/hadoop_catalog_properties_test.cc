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

#include "iceberg/catalog/hadoop/hadoop_catalog_properties.h"

#include <gtest/gtest.h>

namespace iceberg::hadoop {

TEST(HadoopCatalogPropertiesTest, DefaultsMatchJavaCatalogProperties) {
  auto props = HadoopCatalogProperties::default_properties();
  EXPECT_EQ("", props.Get(HadoopCatalogProperties::kName));
  EXPECT_EQ("", props.Get(HadoopCatalogProperties::kIOImpl));
  EXPECT_TRUE(props.Get(HadoopCatalogProperties::kCacheEnabled));
  EXPECT_TRUE(props.Get(HadoopCatalogProperties::kCacheCaseSensitive));
  EXPECT_EQ(30000, props.Get(HadoopCatalogProperties::kCacheExpirationIntervalMs));
  EXPECT_FALSE(props.Get(HadoopCatalogProperties::kIoManifestCacheEnabled));
  EXPECT_EQ(60000,
            props.Get(HadoopCatalogProperties::kIoManifestCacheExpirationIntervalMs));
  EXPECT_EQ(104857600, props.Get(HadoopCatalogProperties::kIoManifestCacheMaxTotalBytes));
  EXPECT_EQ(8388608,
            props.Get(HadoopCatalogProperties::kIoManifestCacheMaxContentLength));
  EXPECT_EQ("in-memory", props.Get(HadoopCatalogProperties::kLockImpl));
  EXPECT_EQ(3000, props.Get(HadoopCatalogProperties::kLockHeartbeatIntervalMs));
  EXPECT_EQ(15000, props.Get(HadoopCatalogProperties::kLockHeartbeatTimeoutMs));
  EXPECT_EQ(4, props.Get(HadoopCatalogProperties::kLockHeartbeatThreads));
  EXPECT_EQ(5000, props.Get(HadoopCatalogProperties::kLockAcquireIntervalMs));
  EXPECT_EQ(180000, props.Get(HadoopCatalogProperties::kLockAcquireTimeoutMs));
  EXPECT_FALSE(props.Get(HadoopCatalogProperties::kSuppressPermissionError));
  EXPECT_EQ("", props.Get(HadoopCatalogProperties::kFsDefaultFS));
  EXPECT_EQ("simple", props.Get(HadoopCatalogProperties::kHadoopSecurityAuthentication));
  EXPECT_EQ("", props.Get(HadoopCatalogProperties::kHadoopKerberosPrincipal));
  EXPECT_EQ("", props.Get(HadoopCatalogProperties::kHadoopKerberosKeytab));
}

TEST(HadoopCatalogPropertiesTest, FromMapOverridesDefaults) {
  auto props = HadoopCatalogProperties::FromMap({
      {"name", "demo"},
      {"warehouse", "file:///tmp/wh"},
      {"lock-impl", "file"},
      {"lock.acquire-timeout-ms", "60000"},
      {"suppress-permission-error", "true"},
      {"hadoop.security.authentication", "kerberos"},
      {"hadoop.kerberos.principal", "svc/_HOST@EXAMPLE.COM"},
  });
  EXPECT_EQ("demo", props.Get(HadoopCatalogProperties::kName));
  EXPECT_EQ("file", props.Get(HadoopCatalogProperties::kLockImpl));
  EXPECT_EQ(60000, props.Get(HadoopCatalogProperties::kLockAcquireTimeoutMs));
  EXPECT_TRUE(props.Get(HadoopCatalogProperties::kSuppressPermissionError));
  EXPECT_EQ("kerberos",
            props.Get(HadoopCatalogProperties::kHadoopSecurityAuthentication));
  EXPECT_EQ("svc/_HOST@EXAMPLE.COM",
            props.Get(HadoopCatalogProperties::kHadoopKerberosPrincipal));
  // Untouched key keeps its default.
  EXPECT_EQ(3000, props.Get(HadoopCatalogProperties::kLockHeartbeatIntervalMs));
}

TEST(HadoopCatalogPropertiesTest, WarehouseRequired) {
  auto missing = HadoopCatalogProperties::default_properties();
  auto missing_result = missing.Warehouse();
  ASSERT_FALSE(missing_result.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, missing_result.error().kind);

  auto empty = HadoopCatalogProperties::FromMap({{"warehouse", ""}});
  auto empty_result = empty.Warehouse();
  ASSERT_FALSE(empty_result.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, empty_result.error().kind);

  auto set = HadoopCatalogProperties::FromMap({{"warehouse", "file:///tmp/wh"}});
  auto set_result = set.Warehouse();
  ASSERT_TRUE(set_result.has_value());
  EXPECT_EQ("file:///tmp/wh", *set_result);
}

TEST(HadoopCatalogPropertiesTest, ValidateAcceptsDefaults) {
  auto props = HadoopCatalogProperties::default_properties();
  EXPECT_TRUE(props.Validate().has_value());
}

TEST(HadoopCatalogPropertiesTest, ValidateRejectsNonPositiveTimeouts) {
  auto bad_acquire = HadoopCatalogProperties::FromMap({
      {"lock.acquire-timeout-ms", "0"},
  });
  auto res = bad_acquire.Validate();
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);

  auto bad_interval = HadoopCatalogProperties::FromMap({
      {"lock.acquire-interval-ms", "-5"},
  });
  res = bad_interval.Validate();
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);
}

TEST(HadoopCatalogPropertiesTest, ValidateRejectsHeartbeatIntervalNotBelowTimeout) {
  auto bad = HadoopCatalogProperties::FromMap({
      {"lock.heartbeat-interval-ms", "5000"},
      {"lock.heartbeat-timeout-ms", "5000"},
  });
  auto res = bad.Validate();
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);
}

TEST(HadoopCatalogPropertiesTest, ValidateRejectsUnknownLockImpl) {
  auto bad = HadoopCatalogProperties::FromMap({
      {"lock-impl", "dynamodb"},
  });
  auto res = bad.Validate();
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);
}

TEST(HadoopCatalogPropertiesTest, ExtractHadoopConfReturnsPrefixedKeysOnly) {
  auto props = HadoopCatalogProperties::FromMap({
      {"warehouse", "hdfs://nn:8020/wh"},
      {"name", "demo"},
      {"lock-impl", "in-memory"},
      {"fs.defaultFS", "hdfs://nn:8020"},
      {"hadoop.security.authentication", "kerberos"},
      {"dfs.nameservices", "ns1"},
      {"dfs.client.failover.proxy.provider.ns1",
       "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"},
  });
  auto extracted = props.ExtractHadoopConf();
  EXPECT_EQ(4, extracted.size());
  EXPECT_TRUE(extracted.contains("fs.defaultFS"));
  EXPECT_TRUE(extracted.contains("hadoop.security.authentication"));
  EXPECT_TRUE(extracted.contains("dfs.nameservices"));
  EXPECT_TRUE(extracted.contains("dfs.client.failover.proxy.provider.ns1"));
  // Catalog-level keys must not leak into the Hadoop pass-through map.
  EXPECT_FALSE(extracted.contains("name"));
  EXPECT_FALSE(extracted.contains("warehouse"));
  EXPECT_FALSE(extracted.contains("lock-impl"));
}

}  // namespace iceberg::hadoop
