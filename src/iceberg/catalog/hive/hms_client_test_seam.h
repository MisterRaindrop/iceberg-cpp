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

#include <memory>

namespace Apache::Hadoop::Hive {
class ThriftHiveMetastoreIf;
}

namespace iceberg::hive {

class HmsClient;
class HmsLockHeartbeat;

/// \brief Test-only seam: build an `HmsClient` whose underlying
///        thrift backend is a caller-supplied fake.
///
/// Production callers use `HmsClient::Connect(config)`. The unit
/// tests in `hms_client_replay_test.cc` subclass
/// `Apache::Hadoop::Hive::ThriftHiveMetastoreNull` and script each
/// RPC's behavior (return a value, throw a specific Thrift exception)
/// so the catch-block plumbing in `hms_client.cc` is reachable
/// without a live Hive Metastore. Defined out-of-line in
/// `hms_client.cc`; this header forward-declares the Thrift type so
/// the broader iceberg_hive public surface keeps no Thrift includes.
std::unique_ptr<HmsClient> HmsClientForTesting(
    std::unique_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreIf> client);

/// \brief Test-only seam: spawn a heartbeat thread against a
///        caller-supplied `HmsClient` instead of opening a real
///        Thrift connection via `HmsClient::Connect`. The returned
///        heartbeat owns its worker thread; the destructor joins it.
std::unique_ptr<HmsLockHeartbeat> HmsLockHeartbeatForTesting(
    std::unique_ptr<HmsClient> client, int64_t lock_id, int32_t interval_ms);

}  // namespace iceberg::hive
