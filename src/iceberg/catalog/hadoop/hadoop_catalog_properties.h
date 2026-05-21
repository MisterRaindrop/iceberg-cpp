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

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

#include "iceberg/catalog/hadoop/iceberg_hadoop_export.h"
#include "iceberg/result.h"
#include "iceberg/util/config.h"

/// \file iceberg/catalog/hadoop/hadoop_catalog_properties.h
/// \brief HadoopCatalogProperties for the file-system based catalog.
///
/// The property keys mirror Apache Iceberg's Java `HadoopCatalog` /
/// `CatalogProperties` so that catalog configuration is portable across
/// language implementations. Where iceberg-cpp does not yet honour a Java
/// behaviour (e.g. the manifest cache layer), the key is still parsed so a
/// shared configuration map does not surface as an error; the implementation
/// note is left next to the entry.

namespace iceberg::hadoop {

/// \brief Configuration class for the Hadoop catalog.
class ICEBERG_HADOOP_EXPORT HadoopCatalogProperties
    : public ConfigBase<HadoopCatalogProperties> {
 public:
  template <typename T>
  using Entry = const ConfigBase<HadoopCatalogProperties>::Entry<T>;

  // -- Catalog identity / FileIO --

  /// \brief The catalog name (also used as the entity id prefix for locks).
  inline static Entry<std::string> kName{"name", ""};
  /// \brief The warehouse base path. Required.
  inline static Entry<std::string> kWarehouse{"warehouse", ""};
  /// \brief The FileIO implementation hint. Empty means auto-detect from the
  /// warehouse scheme (file:// → arrow-fs-local, s3:// → arrow-fs-s3,
  /// hdfs:// → arrow-fs-hdfs once enabled).
  inline static Entry<std::string> kIOImpl{"io-impl", ""};

  // -- Catalog metadata cache (CatalogProperties parity; iceberg-cpp wraps
  // catalog caching at a higher layer if at all, so these are parsed-only). --

  inline static Entry<bool> kCacheEnabled{"cache-enabled", true};
  inline static Entry<bool> kCacheCaseSensitive{"cache.case-sensitive", true};
  inline static Entry<int64_t> kCacheExpirationIntervalMs{"cache.expiration-interval-ms",
                                                          30000};

  // -- Manifest cache (FileIO-layer concern; parsed for config-compat). --

  inline static Entry<bool> kIoManifestCacheEnabled{"io.manifest.cache-enabled", false};
  inline static Entry<int64_t> kIoManifestCacheExpirationIntervalMs{
      "io.manifest.cache.expiration-interval-ms", 60000};
  inline static Entry<int64_t> kIoManifestCacheMaxTotalBytes{
      "io.manifest.cache.max-total-bytes", 104857600};
  inline static Entry<int64_t> kIoManifestCacheMaxContentLength{
      "io.manifest.cache.max-content-length", 8388608};

  // -- LockManager (commit.* equivalents; Java standard key names). --

  /// \brief LockManager implementation. Recognised values: "in-memory" (default)
  /// and "file". Other names are passed through to the LockManager registry.
  inline static Entry<std::string> kLockImpl{"lock-impl", "in-memory"};
  inline static Entry<int64_t> kLockHeartbeatIntervalMs{"lock.heartbeat-interval-ms",
                                                        3000};
  inline static Entry<int64_t> kLockHeartbeatTimeoutMs{"lock.heartbeat-timeout-ms",
                                                       15000};
  inline static Entry<int32_t> kLockHeartbeatThreads{"lock.heartbeat-threads", 4};
  inline static Entry<int64_t> kLockAcquireIntervalMs{"lock.acquire-interval-ms", 5000};
  inline static Entry<int64_t> kLockAcquireTimeoutMs{"lock.acquire-timeout-ms", 180000};

  // -- HadoopCatalog-specific Java behaviours. --

  /// \brief When true, downgrade permission errors from the underlying FileIO
  /// into warnings on list/exists paths instead of returning the error.
  inline static Entry<bool> kSuppressPermissionError{"suppress-permission-error", false};

  // -- HDFS / Kerberos (passthrough into HDFS FileIO when wired in H19/H20). --

  /// \brief The default Hadoop file system (e.g. "hdfs://namenode:8020").
  inline static Entry<std::string> kFsDefaultFS{"fs.defaultFS", ""};
  /// \brief Authentication mode: "simple" or "kerberos".
  inline static Entry<std::string> kHadoopSecurityAuthentication{
      "hadoop.security.authentication", "simple"};
  /// \brief Kerberos principal for the HDFS client (informational; libhdfs
  /// inherits the JVM UGI, so the actual login goes through `kinit`).
  inline static Entry<std::string> kHadoopKerberosPrincipal{"hadoop.kerberos.principal",
                                                            ""};
  /// \brief Path to the Kerberos keytab on disk.
  inline static Entry<std::string> kHadoopKerberosKeytab{"hadoop.kerberos.keytab", ""};

  /// \brief Prefixes that are forwarded verbatim to the HDFS FileIO's
  /// `extra_conf`. Anything starting with these is preserved when calling
  /// ExtractHadoopConf().
  inline static constexpr std::string_view kHadoopPrefix = "hadoop.";
  inline static constexpr std::string_view kDfsPrefix = "dfs.";
  inline static constexpr std::string_view kFsPrefix = "fs.";

  /// \brief Create a default HadoopCatalogProperties instance.
  static HadoopCatalogProperties default_properties();

  /// \brief Create a HadoopCatalogProperties instance from a map of key/value
  /// pairs (typically the user-supplied catalog configuration).
  static HadoopCatalogProperties FromMap(
      std::unordered_map<std::string, std::string> properties);

  /// \brief Get the warehouse location.
  /// \return The warehouse if configured, or InvalidArgument if unset/empty.
  Result<std::string_view> Warehouse() const;

  /// \brief Convenience: collect every property whose key starts with
  /// `hadoop.`, `dfs.`, or `fs.`. Used to forward Hadoop configuration to the
  /// underlying HDFS FileIO. Keys are returned in their original form (no
  /// prefix stripping) so that downstream consumers can pass them straight
  /// into `arrow::fs::HdfsOptions::extra_conf`.
  std::unordered_map<std::string, std::string> ExtractHadoopConf() const;
};

}  // namespace iceberg::hadoop
