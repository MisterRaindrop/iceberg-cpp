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

/// \brief Map any other Thrift transport or protocol exception (caught
///        as `apache::thrift::TException`) to `ErrorKind::kIOError`.
/// `what` should be the exception's `.what()` string.
ICEBERG_HIVE_EXPORT std::unexpected<Error> GenericThriftError(std::string_view context,
                                                              std::string_view what);

}  // namespace iceberg::hive
