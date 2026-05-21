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

#include <string_view>

#include "iceberg/catalog/hive/iceberg_hive_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/hive/hive_errors.h
/// \brief Conversion helpers from Hive Metastore Thrift exceptions to
///        `iceberg::Error`.
///
/// The header deliberately keeps Apache Thrift exception types out of
/// the public API: HmsClient (which already includes the generated
/// Thrift bindings) catches the specific exception subclass, extracts
/// its message field, and forwards both `context` (the originating
/// HMS call, e.g. "get_database") and the message into one of the
/// factories below. The factories then tag the error with the
/// appropriate iceberg `ErrorKind`.

namespace iceberg::hive {

/// \brief Map `NoSuchObjectException` to `ErrorKind::kNotFound`.
///
/// HMS throws this exception generically for any missing database,
/// table, partition or function; HiveCatalog refines it to
/// `kNoSuchNamespace` / `kNoSuchTable` at the call site based on which
/// HMS API was invoked.
ICEBERG_HIVE_EXPORT std::unexpected<Error> NoSuchObjectError(std::string_view context,
                                                             std::string_view message);

/// \brief Map `UnknownDBException` to `ErrorKind::kNoSuchNamespace`.
ICEBERG_HIVE_EXPORT std::unexpected<Error> UnknownDBError(std::string_view context,
                                                          std::string_view message);

/// \brief Map `UnknownTableException` to `ErrorKind::kNoSuchTable`.
ICEBERG_HIVE_EXPORT std::unexpected<Error> UnknownTableError(std::string_view context,
                                                             std::string_view message);

/// \brief Map `AlreadyExistsException` to `ErrorKind::kAlreadyExists`.
ICEBERG_HIVE_EXPORT std::unexpected<Error> AlreadyExistsError(std::string_view context,
                                                              std::string_view message);

/// \brief Map `InvalidObjectException` to `ErrorKind::kInvalidArgument`.
ICEBERG_HIVE_EXPORT std::unexpected<Error> InvalidObjectError(std::string_view context,
                                                              std::string_view message);

/// \brief Map `InvalidPartitionException` to `ErrorKind::kInvalidArgument`.
ICEBERG_HIVE_EXPORT std::unexpected<Error> InvalidPartitionError(
    std::string_view context, std::string_view message);

/// \brief Map `InvalidOperationException` to `ErrorKind::kNotAllowed`.
ICEBERG_HIVE_EXPORT std::unexpected<Error> InvalidOperationError(
    std::string_view context, std::string_view message);

/// \brief Map `ConfigValSecurityException` to `ErrorKind::kNotAuthorized`.
ICEBERG_HIVE_EXPORT std::unexpected<Error> ConfigValSecurityError(
    std::string_view context, std::string_view message);

/// \brief Map `MetaException` (HMS's catch-all internal failure) to
///        `ErrorKind::kIOError`.
ICEBERG_HIVE_EXPORT std::unexpected<Error> MetaError(std::string_view context,
                                                     std::string_view message);

/// \brief Map `apache::thrift::transport::TTransportException` (socket
///        closed, connection refused, frame size limit, etc.) to
///        `ErrorKind::kServiceUnavailable`. Callers that own the
///        `HmsClient` are expected to discard it and reconnect: the
///        underlying transport is no longer usable.
ICEBERG_HIVE_EXPORT std::unexpected<Error> TransportError(std::string_view context,
                                                          std::string_view what);

/// \brief Map any other Thrift protocol exception (caught as
///        `apache::thrift::TException`) to `ErrorKind::kIOError`.
///        `what` should be the exception's `.what()` string.
///        TTransportException should be caught separately via
///        `TransportError` so the pool can react to broken connections.
ICEBERG_HIVE_EXPORT std::unexpected<Error> GenericThriftError(std::string_view context,
                                                              std::string_view what);

/// \brief Re-tag any `kServiceUnavailable` escaping from a mutating
///        catalog op as `kCommitStateUnknown`, **only when the mutating
///        RPC was actually issued**.
///
/// `HmsClientPool::Run`'s reconnect-once contract surfaces
/// `kServiceUnavailable` in two cases that *both* mean "the first
/// mutating RPC was issued but its outcome is undefined":
///   * the initial RPC's transport failed AND the pool's subsequent
///     reconnect attempt also failed (`hms_client_pool.h:Run`'s
///     reconnect-failure branch); or
///   * the initial RPC's transport failed, the reconnect succeeded,
///     and the retry's transport also failed.
///
/// Letting either of those surface verbatim invites the caller to
/// retry the operation, which restarts the lambda with the
/// `*_attempted` flag reset to `false` -- masking a server-side
/// success as a "fresh AlreadyExists / NoSuchTable / etc." that the
/// per-op replay recovery in this file can no longer recognise.
/// Surfacing `kCommitStateUnknown` tells `iceberg::Transaction` (and
/// the user) to stop retrying and inspect HMS to find out what
/// actually happened.
///
/// BUT: some mutating ops do a `GetTable` / `GetDatabase` *before* the
/// mutation as a read-then-write guard. A transport failure inside
/// that pre-read leaves HMS state untouched -- the caller can safely
/// retry. Converting *those* to `kCommitStateUnknown` blocks legitimate
/// retries. The `mutation_attempted` parameter gates the conversion:
/// callers capture a stack-local bool by reference, flip it to `true`
/// immediately before the mutating RPC inside the lambda, and pass it
/// in here. The flag persists across `HmsClientPool::Run`'s reconnect
/// retry, so a first invocation that reached the mutation and a second
/// invocation that bailed out in the pre-read still report
/// `mutation_attempted=true` and correctly funnel through.
///
/// For ops whose lambda's first RPC *is* the mutation
/// (`CreateNamespace`, `CreateTable`, `RegisterTable`), callers pass
/// `true` directly.
///
/// `op_context` should describe the catalog op for diagnostic output
/// (e.g. "HiveCatalog::CreateNamespace(\"warehouse\")"). Read-only
/// ops (`LoadTable`, `ListNamespaces`, ...) should NOT funnel through
/// this helper -- read-only RPCs are idempotent, and turning a
/// retriable transport blip into `kCommitStateUnknown` would only
/// confuse the caller.
template <typename T>
inline Result<T> CommitStateUnknownOnTransportFailure(Result<T> result,
                                                      std::string_view op_context,
                                                      bool mutation_attempted) {
  if (mutation_attempted && !result.has_value() &&
      result.error().kind == ErrorKind::kServiceUnavailable) {
    return CommitStateUnknown(
        "{} encountered HMS transport failure after the mutating RPC was "
        "issued; the operation may or may not have landed (msg={}).",
        op_context, result.error().message);
  }
  return result;
}

}  // namespace iceberg::hive
