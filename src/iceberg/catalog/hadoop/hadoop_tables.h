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
#include <string>
#include <unordered_map>

#include "iceberg/catalog/hadoop/hadoop_catalog_properties.h"
#include "iceberg/catalog/hadoop/iceberg_hadoop_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

/// \file iceberg/catalog/hadoop/hadoop_tables.h
/// \brief HadoopTables: single-table API for filesystem-backed Iceberg tables.

namespace iceberg::hadoop {

/// \brief Single-table API for HadoopCatalog tables.
///
/// `HadoopTables` is the C++ analogue of Java's
/// `org.apache.iceberg.hadoop.HadoopTables`. Where `HadoopCatalog` manages
/// namespaces and table identifiers, `HadoopTables` operates on absolute
/// table paths -- useful when callers know the path on disk but do not
/// want to bring up a full catalog (e.g. ad-hoc loaders, single-table
/// migrations, debugging scripts).
///
/// Tables created or loaded via `HadoopTables` are byte-for-byte
/// compatible with `HadoopCatalog` since both share `hadoop_table_operations`
/// for the commit/refresh protocol.
class ICEBERG_HADOOP_EXPORT HadoopTables {
 public:
  /// \brief Construct from an existing FileIO. The FileIO is reused for
  /// every operation; callers can pre-register Hadoop config via a
  /// `MakeLockManagerWithIO`-style factory.
  explicit HadoopTables(std::shared_ptr<FileIO> file_io,
                        HadoopCatalogProperties config = HadoopCatalogProperties{});

  /// \brief Auto-detect the FileIO from the scheme of the first call.
  ///
  /// Callers who do not have a pre-constructed FileIO can use this overload;
  /// each `Load`/`Create`/`Exists`/`DropTable` call infers `arrow-fs-local`
  /// for `file://`, `arrow-fs-s3` for `s3://`, etc. The lock manager is
  /// shared across calls within the same instance.
  HadoopTables();
  ~HadoopTables();

  /// \brief Load a table from `path`.
  ///
  /// `path` should be the table directory (not the metadata file). Returns
  /// `kNoSuchTable` if `path` does not contain a HadoopCatalog metadata
  /// layout.
  Result<std::shared_ptr<Table>> Load(const std::string& path);

  /// \brief Returns true if `path` resolves to a HadoopCatalog table.
  Result<bool> Exists(const std::string& path);

  /// \brief Create a new table at `path`.
  Result<std::shared_ptr<Table>> Create(
      const std::shared_ptr<Schema>& schema, const std::shared_ptr<PartitionSpec>& spec,
      const std::shared_ptr<SortOrder>& order, const std::string& path,
      const std::unordered_map<std::string, std::string>& properties);

  /// \brief Drop the table at `path`.
  ///
  /// Both purge values currently recursively delete the table directory tree
  /// (see HadoopCatalog::DropTable for the rationale).
  Status DropTable(const std::string& path, bool purge);

  /// \brief Register an externally-written `metadata_file_location` as a
  /// HadoopTable at `path`. Mirrors HadoopCatalog::RegisterTable with the
  /// path-driven entrypoint.
  Result<std::shared_ptr<Table>> RegisterTable(const std::string& path,
                                               const std::string& metadata_file_location);

 private:
  Result<std::shared_ptr<FileIO>> ResolveFileIO(std::string_view path);

  std::shared_ptr<FileIO> file_io_;
  HadoopCatalogProperties config_;
};

}  // namespace iceberg::hadoop
