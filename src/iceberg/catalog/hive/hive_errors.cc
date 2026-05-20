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

namespace iceberg::hive {

std::unexpected<Error> NoSuchObjectError(std::string_view context,
                                         std::string_view message) {
  return NotFound("HMS {} reported object not found: {}", context, message);
}

std::unexpected<Error> UnknownDBError(std::string_view context,
                                      std::string_view message) {
  return NoSuchNamespace("HMS {} reported unknown database: {}", context, message);
}

std::unexpected<Error> UnknownTableError(std::string_view context,
                                         std::string_view message) {
  return NoSuchTable("HMS {} reported unknown table: {}", context, message);
}

std::unexpected<Error> AlreadyExistsError(std::string_view context,
                                          std::string_view message) {
  return AlreadyExists("HMS {} reported object already exists: {}", context, message);
}

std::unexpected<Error> InvalidObjectError(std::string_view context,
                                          std::string_view message) {
  return InvalidArgument("HMS {} rejected request (InvalidObjectException): {}", context,
                         message);
}

std::unexpected<Error> InvalidPartitionError(std::string_view context,
                                             std::string_view message) {
  return InvalidArgument("HMS {} rejected request (InvalidPartitionException): {}",
                         context, message);
}

std::unexpected<Error> InvalidOperationError(std::string_view context,
                                             std::string_view message) {
  return NotAllowed("HMS {} rejected operation (InvalidOperationException): {}", context,
                    message);
}

std::unexpected<Error> ConfigValSecurityError(std::string_view context,
                                              std::string_view message) {
  return NotAuthorized("HMS {} rejected request (ConfigValSecurityException): {}",
                       context, message);
}

std::unexpected<Error> MetaError(std::string_view context, std::string_view message) {
  return IOError("HMS {} failed (MetaException): {}", context, message);
}

std::unexpected<Error> GenericThriftError(std::string_view context,
                                          std::string_view what) {
  return IOError("Thrift error during HMS {}: {}", context, what);
}

}  // namespace iceberg::hive
