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

#include <functional>
#include <string>
#include <string_view>

#include "iceberg/catalog/hadoop/iceberg_hadoop_export.h"

/// \file iceberg/catalog/hadoop/hadoop_log.h
/// \brief Process-global warning sink for HadoopCatalog.
///
/// iceberg-cpp does not currently ship a structured logging facility, so the
/// hadoop catalog had been writing warnings (S3 warehouse, permission
/// suppression) directly to std::cerr. That is awkward for production
/// integrators who already have their own logger.
///
/// `SetWarningHandler` installs a process-global callback that receives
/// each warning. Passing `nullptr` restores the default (std::cerr with a
/// `[iceberg::hadoop]` prefix and trailing newline). The handler is invoked
/// from arbitrary catalog threads, so implementations must be thread-safe.

namespace iceberg::hadoop {

using WarningHandler = std::function<void(std::string_view message)>;

/// \brief Install the process-global warning sink. Pass `nullptr` to revert
/// to the default stderr sink.
ICEBERG_HADOOP_EXPORT void SetWarningHandler(WarningHandler handler);

/// \brief Emit a warning through the currently installed handler.
ICEBERG_HADOOP_EXPORT void LogWarning(std::string_view message);

}  // namespace iceberg::hadoop
