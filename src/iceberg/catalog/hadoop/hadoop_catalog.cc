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

#include "iceberg/catalog/hadoop/hadoop_catalog.h"

#include <format>
#include <optional>
#include <string_view>
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/catalog/hadoop/hadoop_log.h"
#include "iceberg/catalog/hadoop/hadoop_table_operations.h"
#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/statistics_file.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/gzip_internal.h"
#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg::hadoop {

namespace {

// Only LocalFileSystem currently has a true atomic create-if-absent rename
// in iceberg-cpp's arrow adapter (see arrow_io.cc::Rename). All other
// warehouse schemes fall through to a TOCTOU precheck + Move, which means
// HadoopCatalog's metadata-file CAS and the optional `lock-impl=file`
// cross-process guarantee both degrade to best-effort. Emit a runtime
// warning so the foot-gun is visible.
void MaybeWarnNonAtomicWarehouse(std::string_view warehouse) {
  if (IsS3Scheme(warehouse)) {
    LogWarning(std::format(
        "HadoopCatalog on object storage ('{}') has non-atomic rename; concurrent "
        "commits may overwrite each other. Prefer Glue/REST/Nessie for S3 "
        "workloads.",
        warehouse));
    return;
  }
  if (warehouse.starts_with("hdfs://")) {
    LogWarning(std::format(
        "HadoopCatalog on HDFS ('{}'): arrow's HDFS adapter does not expose an "
        "atomic create-if-absent rename, so commit-time CAS and lock-impl=file "
        "cross-process exclusivity are best-effort. Use lock-impl=in-memory for "
        "single-process workloads or layer an external coordination service.",
        warehouse));
  }
}

// suppress-permission-error: downgrade permission failures from the FileIO
// into a log warning + empty result. Mirrors Java's flag of the same name
// (in HadoopCatalog the flag only covers listing / existence checks; commit
// failures still surface).
//
// Arrow's LocalFileSystem wraps EACCES/EPERM into kIOError with the OS
// message ("Permission denied", "Operation not permitted") rather than a
// dedicated kForbidden, so the suppression check matches both the strongly
// typed kinds and the messages embedded in kIOError.
bool SuppressedPermissionError(const HadoopCatalogProperties& config, const Error& err) {
  if (!config.Get(HadoopCatalogProperties::kSuppressPermissionError)) {
    return false;
  }
  auto looks_like_permission = [](const Error& e) {
    if (e.kind == ErrorKind::kForbidden || e.kind == ErrorKind::kNotAuthorized) {
      return true;
    }
    if (e.kind != ErrorKind::kIOError) {
      return false;
    }
    return e.message.find("Permission denied") != std::string::npos ||
           e.message.find("permission denied") != std::string::npos ||
           e.message.find("Operation not permitted") != std::string::npos ||
           e.message.find("operation not permitted") != std::string::npos ||
           e.message.find("AccessDenied") != std::string::npos;
  };
  if (looks_like_permission(err)) {
    LogWarning(std::format("suppress-permission-error: {}", err.message));
    return true;
  }
  return false;
}

// HadoopCatalog::DropTable(purge=true) must satisfy Catalog::DropTable's
// contract -- delete all data files. Without manifest-walking the only way
// to ensure that is to require all data to live under the
// identifier-derived `table_dir`. The checks below enforce that invariant
// at every entry that can stamp a table's effective data location.
Status RejectCustomLocation(std::string_view table_dir, std::string_view location,
                            std::string_view source) {
  if (location.empty()) {
    return {};
  }
  if (LocationUtil::StripTrailingSlash(location) ==
      LocationUtil::StripTrailingSlash(table_dir)) {
    return {};
  }
  return InvalidArgument(
      "{}: explicit table location '{}' must match the warehouse-derived path "
      "'{}' so DropTable(purge=true) can delete the data tree. Pass an empty "
      "string to fall back to the derived path.",
      source, location, table_dir);
}

Status RejectExternalDataPathProperty(
    const std::unordered_map<std::string, std::string>& properties,
    std::string_view source) {
  if (properties.contains(std::string(TableProperties::kWriteDataLocation.key()))) {
    return InvalidArgument(
        "{}: HadoopCatalog forbids '{}' because data outside the table directory "
        "cannot be reached by purge=true without manifest walking. Drop the "
        "property or use a catalog that supports external data paths.",
        source, TableProperties::kWriteDataLocation.key());
  }
  return {};
}

// The lock-impl=file root `<warehouse>/_iceberg_catalog_locks/` only
// collides with a TOP-LEVEL table or namespace of that name -- nested
// `db._iceberg_catalog_locks` is harmless. Reject when the FIRST path
// component (the first namespace level, or the table name for a
// top-level table) equals the reserved lock-root name.
Status RejectTopLevelLockRoot(const Namespace& ns, std::string_view table_name,
                              std::string_view source) {
  const std::string_view first =
      ns.levels.empty() ? table_name : std::string_view(ns.levels.front());
  if (first == hadoop::kLockRootDirName) {
    return InvalidArgument(
        "{}: '{}' is reserved as the warehouse-level lock-impl=file root "
        "directory; a top-level table or namespace cannot use that name.",
        source, hadoop::kLockRootDirName);
  }
  return {};
}

// True iff `ns` denotes the warehouse-level reserved lock-root namespace:
// a single top-level level equal to the lock-root name. A NESTED
// `db._iceberg_catalog_locks` is a legitimate namespace (the reserved-name
// rule is position-aware, see RejectTopLevelLockRoot), so it returns false.
// The namespace read APIs use this to consistently report the reserved
// top-level lock root as "not a namespace".
bool IsTopLevelLockRoot(const Namespace& ns) {
  return ns.levels.size() == 1 && ns.levels.front() == hadoop::kLockRootDirName;
}

}  // namespace

HadoopCatalog::~HadoopCatalog() = default;

Result<std::shared_ptr<HadoopCatalog>> HadoopCatalog::Make(
    std::string_view name, std::shared_ptr<FileIO> file_io,
    HadoopCatalogProperties config) {
  if (file_io == nullptr) {
    return InvalidArgument("HadoopCatalog::Make requires a non-null FileIO.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config.Warehouse());
  ICEBERG_RETURN_UNEXPECTED(config.Validate());
  MaybeWarnNonAtomicWarehouse(warehouse);
  // Canonicalise the warehouse string so two HadoopCatalog instances
  // configured with different SURFACE representations of the same
  // physical location (e.g. `file:///tmp/my%20wh` and
  // `file:///tmp/my wh`) produce the same `table_dir` strings -- which
  // are also the in-memory lock keys. Without this, peer catalogs
  // would publish to the same directory while holding distinct locks,
  // re-opening the codec-mismatch CAS race the lock is meant to close.
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse_str, hadoop::CanonicalizeWarehouse(warehouse));
  ICEBERG_ASSIGN_OR_RAISE(auto lock_manager,
                          hadoop::MakeLockManagerWithIO(config, file_io));

  return std::shared_ptr<HadoopCatalog>(new HadoopCatalog(
      std::string(name), std::move(file_io), std::move(config),
      std::shared_ptr<LockManager>(std::move(lock_manager)), std::move(warehouse_str)));
}

Result<std::shared_ptr<HadoopCatalog>> HadoopCatalog::Make(
    std::string_view name, HadoopCatalogProperties config) {
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config.Warehouse());

  // Select the FileIO factory keyed by warehouse scheme. arrow-fs-local /
  // arrow-fs-s3 / arrow-fs-hdfs are populated by iceberg_bundle's static
  // initialiser; if the caller did not link iceberg_bundle (or call
  // EnsureArrowFileIOsRegistered() themselves) the Load below will surface
  // a clear kNotSupported error.
  std::string io_name(FileIORegistry::kArrowLocalFileIO);
  if (warehouse.starts_with("hdfs://")) {
    io_name = std::string(FileIORegistry::kArrowHdfsFileIO);
    // P1: parse the warehouse URI authority (host[:port]) and inject it as
    // fs.defaultFS if the caller did not provide one explicitly. Without
    // this, arrow's HdfsOptions defaults to the JVM's core-site value and
    // the user-supplied namenode in `warehouse=hdfs://nn:8020/wh` would be
    // silently ignored.
    constexpr std::string_view kHdfsPrefix = "hdfs://";
    std::string_view tail = warehouse;
    tail.remove_prefix(kHdfsPrefix.size());
    const auto authority_end = tail.find('/');
    const std::string_view authority =
        authority_end == std::string_view::npos ? tail : tail.substr(0, authority_end);
    if (!authority.empty()) {
      auto& configs = config.mutable_configs();
      auto fs_default =
          configs.find(std::string(HadoopCatalogProperties::kFsDefaultFS.key()));
      if (fs_default == configs.end() || fs_default->second.empty()) {
        configs[std::string(HadoopCatalogProperties::kFsDefaultFS.key())] =
            std::format("hdfs://{}", authority);
      }
    }
  } else if (IsS3Scheme(warehouse)) {
    io_name = std::string(FileIORegistry::kArrowS3FileIO);
  }

  // Allow an explicit override -- this is also how callers can opt into a
  // custom FileIO that natively understands s3a:// / s3n://.
  const auto override_impl = config.Get(HadoopCatalogProperties::kIOImpl);
  if (!override_impl.empty()) {
    io_name = override_impl;
  }

  // arrow-fs-s3 only accepts the canonical `s3://` scheme. Reject the
  // JVM-only Hadoop aliases now -- but only if the final FileIO is
  // arrow-fs-s3. A caller who set io-impl to a custom FileIO that does
  // understand s3a/s3n is free to keep using them.
  if (io_name == FileIORegistry::kArrowS3FileIO &&
      (warehouse.starts_with("s3a://") || warehouse.starts_with("s3n://"))) {
    return InvalidArgument(
        "HadoopCatalog::Make: arrow-fs-s3 only accepts 's3://' URIs; the "
        "warehouse '{}' uses a JVM-only Hadoop alias. Use 's3://', or set "
        "'io-impl' to a custom FileIO that supports the alias.",
        warehouse);
  }

  auto file_io_result = FileIORegistry::Load(io_name, config.configs());
  if (!file_io_result.has_value()) {
    const auto& err = file_io_result.error();
    if (err.kind == ErrorKind::kNotImplemented || err.kind == ErrorKind::kNotFound) {
      return InvalidArgument(
          "HadoopCatalog::Make: FileIO '{}' is not registered. The auto-detect "
          "Make(name, config) overload relies on iceberg_bundle to register the "
          "arrow-backed FileIOs at static-init time. Either (a) link your binary "
          "against iceberg_bundle (the auto-registration runs automatically), "
          "(b) call iceberg::arrow::EnsureArrowFileIOsRegistered() before this "
          "Make() call, or (c) use the Make(name, file_io, config) overload to "
          "pass an explicit FileIO instance. Underlying error: {}",
          io_name, err.message);
    }
    return std::unexpected<Error>(err);
  }
  return Make(name, std::shared_ptr<FileIO>(std::move(*file_io_result)),
              std::move(config));
}

HadoopCatalog::HadoopCatalog(std::string name, std::shared_ptr<FileIO> file_io,
                             HadoopCatalogProperties config,
                             std::shared_ptr<LockManager> lock_manager,
                             std::string warehouse)
    : name_(std::move(name)),
      file_io_(std::move(file_io)),
      config_(std::move(config)),
      lock_manager_(std::move(lock_manager)),
      warehouse_(std::move(warehouse)) {}

std::string_view HadoopCatalog::name() const { return name_; }

Status HadoopCatalog::CreateNamespace(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  // Java HadoopCatalog rejects any non-empty metadata payload. We mirror that
  // contract here so cross-language users do not silently lose data.
  if (!properties.empty()) {
    return NotSupported(
        "HadoopCatalog::CreateNamespace does not support namespace properties; "
        "Java HadoopCatalog rejects this configuration.");
  }
  if (ns.levels.empty()) {
    return InvalidArgument(
        "HadoopCatalog::CreateNamespace requires a non-empty namespace.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, hadoop::NamespaceDir(warehouse_, ns));

  ICEBERG_RETURN_UNEXPECTED(
      RejectTopLevelLockRoot(ns, /*table_name=*/{}, "HadoopCatalog::CreateNamespace"));
  // Reject if any proper ancestor of `ns` is already a Hadoop table.
  // Creating a namespace inside an existing table would let a later
  // DropTable(purge=true) of that table wipe the namespace and any
  // nested tables along with it.
  ICEBERG_RETURN_UNEXPECTED(
      hadoop::RejectAncestorIsTable(*file_io_, warehouse_, ns, /*last_inclusive=*/false));

  // Serialize against concurrent CreateTable at any ancestor path -- the
  // ancestor check above is racy on its own because CreateTable uses a
  // DIFFERENT per-table lock (keyed at its own table_dir). Without the
  // lock + post-create recheck, a concurrent CreateTable(db.parent)
  // could publish while we're between the ancestor scan and CreateDir,
  // and the resulting db.parent.sub namespace would live INSIDE a
  // table that DropTable(purge=true) recursively wipes.
  const std::string owner =
      name_ + ":create-ns:" + ns.ToString() + ":" + Uuid::GenerateV7().ToString();
  ICEBERG_ASSIGN_OR_RAISE(auto acquired, lock_manager_->Acquire(ns_dir, owner));
  if (!acquired) {
    return CommitFailed(
        "HadoopCatalog::CreateNamespace: failed to acquire lock for '{}' within "
        "the configured timeout.",
        ns_dir);
  }
  LockReleaseGuard guard(lock_manager_.get(), ns_dir, owner);

  ICEBERG_ASSIGN_OR_RAISE(auto already, file_io_->Exists(ns_dir));
  if (already) {
    // Path may have been claimed by a table. Surface the conflict rather
    // than silently treating the table as a namespace.
    ICEBERG_ASSIGN_OR_RAISE(auto is_table, hadoop::IsHadoopTableDir(*file_io_, ns_dir));
    if (is_table) {
      return AlreadyExists(
          "Cannot create namespace '{}' at {}: a table already occupies that path.",
          ns.ToString(), ns_dir);
    }
    return AlreadyExists("Namespace '{}' already exists at {}.", ns.ToString(), ns_dir);
  }
  ICEBERG_RETURN_UNEXPECTED(file_io_->CreateDir(ns_dir));
  // Post-create recheck: a concurrent CreateTable at an ancestor may
  // have published while we were holding only the leaf-namespace lock.
  // If so, our just-created dir lives INSIDE a table -- rollback so
  // DropTable(purge=true) of the ancestor table cannot wipe stranded
  // catalog state.
  if (auto check = hadoop::RejectAncestorIsTable(*file_io_, warehouse_, ns,
                                                 /*last_inclusive=*/false);
      !check.has_value()) {
    std::ignore = file_io_->DeleteDir(ns_dir, /*recursive=*/false);
    return std::unexpected<Error>(check.error());
  }
  return {};
}

Result<std::vector<Namespace>> HadoopCatalog::ListNamespaces(const Namespace& ns) const {
  // The reserved top-level lock root is never a namespace, so listing
  // "under" it must fail like any non-existent namespace -- symmetric with
  // NamespaceExists/GetNamespaceProperties/DropNamespace and ListTables.
  if (IsTopLevelLockRoot(ns)) {
    return NoSuchNamespace(
        "Namespace '{}' does not exist (reserved lock-impl=file root directory).",
        ns.ToString());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto parent_dir, hadoop::NamespaceDir(warehouse_, ns));

  // Listing the warehouse root when ns is empty is allowed; otherwise the
  // parent must exist.
  auto parent_exists_result = file_io_->Exists(parent_dir);
  if (!parent_exists_result.has_value()) {
    if (SuppressedPermissionError(config_, parent_exists_result.error())) {
      return std::vector<Namespace>{};
    }
    return std::unexpected<Error>(parent_exists_result.error());
  }
  if (!*parent_exists_result) {
    if (ns.levels.empty()) {
      return std::vector<Namespace>{};
    }
    return NoSuchNamespace("Namespace '{}' does not exist.", ns.ToString());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto is_dir, file_io_->IsDirectory(parent_dir));
  if (!is_dir) {
    return NoSuchNamespace("Namespace '{}' is not a directory.", ns.ToString());
  }
  // The path itself must not be a table -- enumerating a table's
  // internal subdirs would surface `metadata/` as a fake child
  // namespace (and the result `db.events.metadata` then fails the
  // validator because `metadata` is reserved).
  if (!ns.levels.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto is_table,
                            hadoop::IsHadoopTableDir(*file_io_, parent_dir));
    if (is_table) {
      return NoSuchNamespace(
          "Namespace '{}' does not exist (path is a Hadoop table at {}).", ns.ToString(),
          parent_dir);
    }
  }

  auto entries_result = file_io_->ListDir(parent_dir);
  if (!entries_result.has_value()) {
    if (SuppressedPermissionError(config_, entries_result.error())) {
      return std::vector<Namespace>{};
    }
    return std::unexpected<Error>(entries_result.error());
  }
  const auto& entries = *entries_result;
  std::vector<Namespace> children;
  for (const auto& entry : entries) {
    if (!entry.is_directory) {
      continue;
    }
    const std::string_view leaf = hadoop::Basename(entry.location);
    if (leaf.empty()) {
      continue;
    }
    // Hide the catalog's own lock-root directory
    // (`<warehouse>/_iceberg_catalog_locks/`, used by lock-impl=file), but
    // ONLY at the warehouse root. The reserved-name rule is position-aware
    // (see RejectTopLevelLockRoot): a NESTED `db._iceberg_catalog_locks` is
    // a legitimate, creatable namespace, so it must remain listable here.
    // Filtering it unconditionally would make the directory un-listable
    // even though CreateNamespace allowed it.
    if (ns.levels.empty() && leaf == hadoop::kLockRootDirName) {
      continue;
    }
    // Skip directories that look like tables. The strict check (metadata/
    // exists AND contains at least one v{N}.metadata.json) is required for
    // correctness -- a "fast" Exists(metadata/) probe would false-positive
    // on a legitimate namespace called `metadata` or on a half-created
    // table that hasn't published v1 yet.
    auto is_table_result = hadoop::IsHadoopTableDir(*file_io_, entry.location);
    if (!is_table_result.has_value()) {
      if (SuppressedPermissionError(config_, is_table_result.error())) {
        // Cannot classify this child; skip it from the listing instead of
        // failing the whole call. Mirrors Java's behaviour when the flag is
        // set and a child's metadata/ is unreadable.
        continue;
      }
      return std::unexpected<Error>(is_table_result.error());
    }
    if (*is_table_result) {
      continue;
    }
    Namespace child;
    child.levels = ns.levels;
    child.levels.emplace_back(leaf);
    children.push_back(std::move(child));
  }
  return children;
}

Result<std::unordered_map<std::string, std::string>>
HadoopCatalog::GetNamespaceProperties(const Namespace& ns) const {
  // The top-level lock-root directory is reserved file-lock storage, not a
  // namespace. A file-lock run may have created it on disk, but it must
  // never surface as a namespace -- symmetric with ListNamespaces filtering
  // it out and CreateNamespace rejecting it.
  if (IsTopLevelLockRoot(ns)) {
    return NoSuchNamespace(
        "Namespace '{}' does not exist (reserved lock-impl=file root directory).",
        ns.ToString());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, hadoop::NamespaceDir(warehouse_, ns));
  auto exists_result = file_io_->Exists(ns_dir);
  if (!exists_result.has_value()) {
    if (SuppressedPermissionError(config_, exists_result.error())) {
      return std::unordered_map<std::string, std::string>{};
    }
    return std::unexpected<Error>(exists_result.error());
  }
  if (!*exists_result) {
    return NoSuchNamespace("Namespace '{}' does not exist.", ns.ToString());
  }
  // A directory that looks like a HadoopCatalog table is not a namespace --
  // surfacing it as one would lie to ListNamespaces/NamespaceExists and
  // potentially shadow a real catalog table.
  ICEBERG_ASSIGN_OR_RAISE(auto is_table, hadoop::IsHadoopTableDir(*file_io_, ns_dir));
  if (is_table) {
    return NoSuchNamespace(
        "Namespace '{}' does not exist (path is occupied by a table at {}).",
        ns.ToString(), ns_dir);
  }
  // Java HadoopCatalog returns a single-entry map keyed by "location". We
  // mirror that behaviour exactly.
  return std::unordered_map<std::string, std::string>{{"location", ns_dir}};
}

Status HadoopCatalog::DropNamespace(const Namespace& ns) {
  if (ns.levels.empty()) {
    return InvalidArgument(
        "HadoopCatalog::DropNamespace requires a non-empty namespace.");
  }
  // The reserved top-level lock root is not a namespace, so DropNamespace
  // must not delete it (which would destroy active file locks). Report it
  // as non-existent, symmetric with NamespaceExists/GetNamespaceProperties.
  if (IsTopLevelLockRoot(ns)) {
    return NoSuchNamespace(
        "Namespace '{}' does not exist (reserved lock-impl=file root directory).",
        ns.ToString());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, hadoop::NamespaceDir(warehouse_, ns));
  ICEBERG_ASSIGN_OR_RAISE(auto exists, file_io_->Exists(ns_dir));
  if (!exists) {
    return NoSuchNamespace("Namespace '{}' does not exist.", ns.ToString());
  }
  // Defensive symmetric guard with GetNamespaceProperties/NamespaceExists:
  // if the path is actually a table, DropNamespace must not silently delete
  // the table directory. Surface it as "not a namespace" instead.
  ICEBERG_ASSIGN_OR_RAISE(auto is_table, hadoop::IsHadoopTableDir(*file_io_, ns_dir));
  if (is_table) {
    return NoSuchNamespace(
        "Namespace '{}' does not exist (path is occupied by a table at {}); "
        "use DropTable to remove it.",
        ns.ToString(), ns_dir);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto entries, file_io_->ListDir(ns_dir));
  if (!entries.empty()) {
    return NamespaceNotEmpty("Namespace '{}' is not empty.", ns.ToString());
  }
  ICEBERG_RETURN_UNEXPECTED(file_io_->DeleteDir(ns_dir, /*recursive=*/false));
  return {};
}

Result<bool> HadoopCatalog::NamespaceExists(const Namespace& ns) const {
  // The reserved top-level lock root is never a namespace, even if a
  // file-lock run created the directory on disk.
  if (IsTopLevelLockRoot(ns)) {
    return false;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, hadoop::NamespaceDir(warehouse_, ns));
  auto exists_result = file_io_->Exists(ns_dir);
  if (!exists_result.has_value()) {
    if (SuppressedPermissionError(config_, exists_result.error())) {
      return false;
    }
    return std::unexpected<Error>(exists_result.error());
  }
  if (!*exists_result) {
    return false;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto is_dir, file_io_->IsDirectory(ns_dir));
  if (!is_dir) {
    return false;
  }
  // A namespace directory that holds a metadata/ folder with versioned
  // metadata is actually a table; report it as not a namespace.
  ICEBERG_ASSIGN_OR_RAISE(auto is_table, hadoop::IsHadoopTableDir(*file_io_, ns_dir));
  return !is_table;
}

Status HadoopCatalog::UpdateNamespaceProperties(
    const Namespace& /*ns*/,
    const std::unordered_map<std::string, std::string>& /*updates*/,
    const std::unordered_set<std::string>& /*removals*/) {
  // Java HadoopCatalog throws UnsupportedOperationException for both
  // setProperties and removeProperties. We expose the same contract.
  return NotSupported(
      "HadoopCatalog::UpdateNamespaceProperties is not supported; "
      "Java HadoopCatalog rejects namespace property mutation.");
}

Result<std::vector<TableIdentifier>> HadoopCatalog::ListTables(
    const Namespace& ns) const {
  // The reserved top-level lock root is not a namespace; refuse to
  // enumerate tables under it rather than returning an empty list.
  if (IsTopLevelLockRoot(ns)) {
    return NoSuchNamespace(
        "Namespace '{}' does not exist (reserved lock-impl=file root directory).",
        ns.ToString());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, hadoop::NamespaceDir(warehouse_, ns));
  auto ns_exists_result = file_io_->Exists(ns_dir);
  if (!ns_exists_result.has_value()) {
    if (SuppressedPermissionError(config_, ns_exists_result.error())) {
      return std::vector<TableIdentifier>{};
    }
    return std::unexpected<Error>(ns_exists_result.error());
  }
  if (!*ns_exists_result) {
    if (ns.levels.empty()) {
      return std::vector<TableIdentifier>{};
    }
    return NoSuchNamespace("Namespace '{}' does not exist.", ns.ToString());
  }
  // Refuse to enumerate "tables" under a path that is itself a table --
  // otherwise we'd report its `metadata/` / `data/` subdirs as if they
  // were sibling tables. Same guard ListNamespaces has.
  if (!ns.levels.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto is_table, hadoop::IsHadoopTableDir(*file_io_, ns_dir));
    if (is_table) {
      return NoSuchNamespace(
          "Namespace '{}' does not exist (path is a Hadoop table at {}).", ns.ToString(),
          ns_dir);
    }
  }

  auto entries_result = file_io_->ListDir(ns_dir);
  if (!entries_result.has_value()) {
    if (SuppressedPermissionError(config_, entries_result.error())) {
      return std::vector<TableIdentifier>{};
    }
    return std::unexpected<Error>(entries_result.error());
  }
  const auto& entries = *entries_result;
  std::vector<TableIdentifier> tables;
  for (const auto& entry : entries) {
    if (!entry.is_directory) {
      continue;
    }
    // Strict check: a table dir has a metadata/ subdir AND contains at
    // least one v{N}.metadata.json. Cheaper "metadata/ exists" probes would
    // list half-created tables that TableExists/DropTable later refuse to
    // recognise, breaking listing/load symmetry.
    auto is_table_result = hadoop::IsHadoopTableDir(*file_io_, entry.location);
    if (!is_table_result.has_value()) {
      if (SuppressedPermissionError(config_, is_table_result.error())) {
        // Cannot classify this child; skip it from the listing instead of
        // failing the whole call.
        continue;
      }
      return std::unexpected<Error>(is_table_result.error());
    }
    if (!*is_table_result) {
      continue;
    }
    tables.push_back(
        TableIdentifier{.ns = ns, .name = std::string(hadoop::Basename(entry.location))});
  }
  return tables;
}

namespace {

// Atomically publish a v1 metadata file + version-hint.text under
// `table_dir`. `produce_bytes` returns the bytes to write (TableMetadata
// serialisation happens at the caller's discretion); `codec` selects the
// filename suffix. Caller holds the lock; this helper only does the
// publish + version-hint update.
Result<std::string> PublishV1AfterLock(FileIO& file_io, const std::string& table_dir,
                                       MetadataCompressionCodec codec,
                                       std::string_view bytes) {
  const std::string metadata_dir = hadoop::MetadataDir(table_dir);
  const std::string metadata_path =
      hadoop::MetadataFilePath(table_dir, /*version=*/1, codec);
  const std::string temp_path = std::format("{}/{}-v1.metadata.json.tmp", metadata_dir,
                                            Uuid::GenerateV7().ToString());

  if (auto write_result = file_io.WriteFile(temp_path, bytes);
      !write_result.has_value()) {
    std::ignore = file_io.DeleteFile(temp_path);
    return std::unexpected<Error>(write_result.error());
  }
  if (auto rename_result = file_io.Rename(temp_path, metadata_path, /*overwrite=*/false);
      !rename_result.has_value()) {
    std::ignore = file_io.DeleteFile(temp_path);
    // Only the "destination exists" failure means a table is already
    // there; permission errors, NotSupported, generic IOError must be
    // surfaced as themselves so the operator can diagnose them.
    if (rename_result.error().kind == ErrorKind::kAlreadyExists) {
      return AlreadyExists(
          "HadoopCatalog: v1.metadata.json already exists at {} "
          "(possibly an orphan from a crashed creator).",
          metadata_path);
    }
    return std::unexpected<Error>(rename_result.error());
  }

  // The rename above IS the commit point for v1: from this moment on
  // ResolveCurrentMetadata (which prefers max(hint, listdir-max)) will
  // surface this file as authoritative. The hint update below is a
  // best-effort fast-path; if it fails we MUST NOT delete metadata_path,
  // since doing so would roll back a committed publish and confuse any
  // concurrent reader that has already observed it. Log and return
  // success with a stale hint -- Refresh handles the lag via listdir.
  const std::string hint_path = hadoop::VersionHintPath(table_dir);
  const std::string hint_tmp =
      std::format("{}.tmp.{}", hint_path, Uuid::GenerateV7().ToString());
  if (auto hint_write = file_io.WriteFile(hint_tmp, std::string("1\n"));
      !hint_write.has_value()) {
    LogWarning(std::format(
        "HadoopCatalog: v1 published at '{}' but version-hint.text write failed: "
        "{}; Refresh will recover via listdir fallback.",
        metadata_path, hint_write.error().message));
    return metadata_path;
  }
  if (auto hint_rename = file_io.Rename(hint_tmp, hint_path, /*overwrite=*/true);
      !hint_rename.has_value()) {
    std::ignore = file_io.DeleteFile(hint_tmp);
    LogWarning(std::format(
        "HadoopCatalog: v1 published at '{}' but version-hint.text rename failed: "
        "{}; Refresh will recover via listdir fallback.",
        metadata_path, hint_rename.error().message));
    return metadata_path;
  }
  return metadata_path;
}

}  // namespace

Result<std::string> HadoopCatalog::WriteInitialTableLocked(
    const std::string& table_dir, const TableMetadata& metadata,
    const std::string& owner_prefix) {
  // Path-based tables cannot redirect metadata to an external location.
  // Same rule HadoopTableOperations::Commit applies to UpdateTable; we
  // enforce it on the create paths too so a user-supplied
  // write.metadata.path can't pin the wrong metadata directory at v1.
  if (metadata.properties.configs().contains(
          TableProperties::kWriteMetadataLocation.key())) {
    return InvalidArgument(
        "Hadoop path-based tables cannot set '{}'; the metadata directory is fixed "
        "under the table location.",
        TableProperties::kWriteMetadataLocation.key());
  }
  // Same rationale for `write.data.path`: data outside the table dir
  // escapes DropTable(purge=true)'s recursive delete.
  ICEBERG_RETURN_UNEXPECTED(RejectExternalDataPathProperty(
      metadata.properties.configs(), "HadoopCatalog::WriteInitialTableLocked"));
  // The metadata's `location` field is what data writers actually consult;
  // make sure it matches the warehouse-derived table directory.
  ICEBERG_RETURN_UNEXPECTED(RejectCustomLocation(
      table_dir, metadata.location, "HadoopCatalog::WriteInitialTableLocked"));
  ICEBERG_ASSIGN_OR_RAISE(auto codec, hadoop::ResolveCommitCodec(metadata));
  ICEBERG_ASSIGN_OR_RAISE(auto bytes, hadoop::EncodeMetadataWithCodec(metadata, codec));
  return WriteInitialBytesLocked(table_dir, bytes, codec, owner_prefix);
}

Result<std::string> HadoopCatalog::WriteInitialBytesLocked(
    const std::string& table_dir, std::string_view raw_bytes,
    MetadataCompressionCodec codec, const std::string& owner_prefix) {
  const std::string owner =
      name_ + ":" + owner_prefix + ":" + Uuid::GenerateV7().ToString();
  // Acquire BEFORE creating the metadata directory: a concurrent
  // DropTable on the same path can be in its post-Release rmdir(metadata)
  // window. If we CreateDir(metadata) first and then block on Acquire,
  // the drop's rmdir can wipe our freshly-created dir before our
  // Acquire even gets a chance, and the later WriteFile fails for
  // missing parent. Acquiring first serialises us against the drop:
  // with the file-lock backend, Acquire itself CreateDir's metadata
  // (idempotent) while writing the lock body; with the in-memory
  // backend, we do the CreateDir below under our held lock.
  ICEBERG_ASSIGN_OR_RAISE(auto acquired, lock_manager_->Acquire(table_dir, owner));
  if (!acquired) {
    return CommitFailed(
        "HadoopCatalog: failed to acquire table lock for '{}' within the configured "
        "timeout.",
        table_dir);
  }
  LockReleaseGuard guard(lock_manager_.get(), table_dir, owner);
  // Idempotent under file-lock (Acquire already created the dir);
  // required under in-memory-lock where Acquire has no on-disk side
  // effects.
  ICEBERG_RETURN_UNEXPECTED(file_io_->CreateDir(hadoop::MetadataDir(table_dir)));

  ICEBERG_ASSIGN_OR_RAISE(auto recheck, hadoop::IsHadoopTableDir(*file_io_, table_dir));
  if (recheck) {
    return AlreadyExists("Table already exists at {}.", table_dir);
  }
  // Also re-check the namespace-occupation invariant under the lock: a
  // concurrent CreateNamespace could have populated this path between the
  // pre-check and Acquire. Without this guard a CreateTable would either
  // bury the namespace's children under a new metadata/ or fight a parallel
  // create at a different leaf inside the namespace.
  ICEBERG_ASSIGN_OR_RAISE(auto occupied,
                          hadoop::HasNonTableInternalChildren(*file_io_, table_dir));
  if (occupied) {
    return InvalidArgument(
        "Cannot create table at {}: path is occupied by a namespace (it already "
        "contains child entries other than metadata/ and data/).",
        table_dir);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto published,
                          PublishV1AfterLock(*file_io_, table_dir, codec, raw_bytes));

  // Post-publish hierarchy recheck. The per-table lock we hold only
  // serializes operations on THIS table_dir; a concurrent CreateTable
  // at an ancestor (e.g. our parent namespace) or descendant of our
  // path uses a DIFFERENT lock and may have passed its own pre-publish
  // ancestor scan while we did the same. By rechecking the hierarchy
  // immediately after our v1 lands, BOTH writers in a parent/child
  // race observe the conflict and both rollback -- the caller then
  // retries, and only one survives the second pass. Without this step
  // a concurrent CreateTable(db.team) + CreateTable(db.team.sub.child)
  // could leave the catalog in a state where DropTable(db.team,
  // purge=true) silently wipes the child.
  ICEBERG_ASSIGN_OR_RAISE(auto descendant_occupied,
                          hadoop::HasNonTableInternalChildren(*file_io_, table_dir));
  bool conflict = descendant_occupied;
  std::string conflict_msg;
  if (descendant_occupied) {
    conflict_msg = std::format(
        "post-publish recheck: '{}' acquired a non-table-internal child during "
        "our publish window (concurrent descendant CreateTable / "
        "CreateNamespace).",
        table_dir);
  }
  if (!conflict) {
    // Walk parents of table_dir up to the warehouse and refuse if any
    // ancestor became a Hadoop table during our publish window. Strip
    // the warehouse prefix and walk the remaining ancestor segments.
    const std::string_view warehouse_view = LocationUtil::StripTrailingSlash(warehouse_);
    const std::string_view table_view = LocationUtil::StripTrailingSlash(table_dir);
    if (table_view.starts_with(warehouse_view) &&
        table_view.size() > warehouse_view.size() + 1) {
      std::string_view rel = table_view.substr(warehouse_view.size() + 1);
      // Walk every proper prefix of `rel`.
      size_t cursor = rel.find('/');
      while (cursor != std::string_view::npos) {
        const std::string ancestor =
            std::string(warehouse_view) + "/" + std::string(rel.substr(0, cursor));
        auto is_table = hadoop::IsHadoopTableDir(*file_io_, ancestor);
        if (is_table.has_value() && *is_table) {
          conflict = true;
          conflict_msg = std::format(
              "post-publish recheck: ancestor '{}' became a Hadoop table during "
              "our publish window (concurrent ancestor CreateTable).",
              ancestor);
          break;
        }
        cursor = rel.find('/', cursor + 1);
      }
    }
  }
  if (conflict) {
    // Rollback: best-effort delete of the just-published v1 and the
    // version-hint we wrote. Both writers in the race do this; the
    // caller sees CommitFailed and retries -- only one of them then
    // re-publishes cleanly.
    std::ignore = file_io_->DeleteFile(published);
    std::ignore = file_io_->DeleteFile(hadoop::VersionHintPath(table_dir));
    return CommitFailed("{}", conflict_msg);
  }
  return published;
}

Result<std::shared_ptr<Table>> HadoopCatalog::CreateTable(
    const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  if (schema == nullptr || spec == nullptr || order == nullptr) {
    return InvalidArgument(
        "HadoopCatalog::CreateTable requires non-null schema, spec, and order.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse_, identifier));
  ICEBERG_RETURN_UNEXPECTED(RejectTopLevelLockRoot(identifier.ns, identifier.name,
                                                   "HadoopCatalog::CreateTable"));
  ICEBERG_RETURN_UNEXPECTED(
      RejectCustomLocation(table_dir, location, "HadoopCatalog::CreateTable"));
  ICEBERG_RETURN_UNEXPECTED(
      RejectExternalDataPathProperty(properties, "HadoopCatalog::CreateTable"));
  // Refuse to nest a table inside an existing table directory --
  // DropTable(purge=true) on the outer table would recursively wipe
  // this one. The leaf (table_dir itself) is checked separately below
  // via IsHadoopTableDir.
  ICEBERG_RETURN_UNEXPECTED(hadoop::RejectAncestorIsTable(
      *file_io_, warehouse_, identifier.ns, /*last_inclusive=*/true));

  // Java's HadoopCatalog rejects creation when the table directory already
  // looks like an Iceberg table. The check is repeated under the lock inside
  // WriteInitialTableLocked; doing it here first lets us avoid acquiring the
  // lock on the common no-op case.
  ICEBERG_ASSIGN_OR_RAISE(auto already_table,
                          hadoop::IsHadoopTableDir(*file_io_, table_dir));
  if (already_table) {
    return AlreadyExists("Table '{}' already exists at {}.", identifier.ToString(),
                         table_dir);
  }
  // Refuse to create a table under a directory that already holds namespace
  // children -- otherwise ListNamespaces / NamespaceExists would start
  // hiding the path (it now passes IsHadoopTableDir) while its sub-entries
  // remain on disk in a half-reachable state. The same check runs again
  // under the lock for race safety; this is the cheap path.
  ICEBERG_ASSIGN_OR_RAISE(auto path_occupied,
                          hadoop::HasNonTableInternalChildren(*file_io_, table_dir));
  if (path_occupied) {
    return InvalidArgument(
        "Cannot create table '{}' at {}: path is occupied by a namespace.",
        identifier.ToString(), table_dir);
  }

  const std::string base_location = location.empty() ? table_dir : location;
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, TableMetadata::Make(*schema, *spec, *order,
                                                             base_location, properties));

  ICEBERG_ASSIGN_OR_RAISE(
      auto metadata_path,
      WriteInitialTableLocked(table_dir, *metadata, "create:" + identifier.ToString()));

  std::shared_ptr<TableMetadata> metadata_ptr = std::move(metadata);
  return Table::Make(identifier, metadata_ptr, metadata_path, file_io_,
                     shared_from_this());
}

Result<std::shared_ptr<Table>> HadoopCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<TableRequirement>>& requirements,
    const std::vector<std::unique_ptr<TableUpdate>>& updates) {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse_, identifier));

  ICEBERG_ASSIGN_OR_RAISE(auto is_create, TableRequirements::IsCreate(requirements));

  std::shared_ptr<TableMetadata> base;
  std::unique_ptr<TableMetadataBuilder> builder;
  std::optional<HadoopTableOperations> ops;
  if (is_create) {
    // Fast pre-check before paying for an Acquire on a known-existing table;
    // the lock-then-recheck inside WriteInitialTableLocked is still the
    // correctness guard.
    ICEBERG_ASSIGN_OR_RAISE(auto already_table,
                            hadoop::IsHadoopTableDir(*file_io_, table_dir));
    if (already_table) {
      return AlreadyExists("Table '{}' already exists at {}.", identifier.ToString(),
                           table_dir);
    }
    ICEBERG_RETURN_UNEXPECTED(RejectTopLevelLockRoot(identifier.ns, identifier.name,
                                                     "HadoopCatalog::UpdateTable"));
    // Reject nesting inside an existing table.
    ICEBERG_RETURN_UNEXPECTED(hadoop::RejectAncestorIsTable(
        *file_io_, warehouse_, identifier.ns, /*last_inclusive=*/true));
    // Same namespace-occupation guard as CreateTable; the lock-time recheck
    // inside WriteInitialTableLocked covers the race window.
    ICEBERG_ASSIGN_OR_RAISE(auto path_occupied,
                            hadoop::HasNonTableInternalChildren(*file_io_, table_dir));
    if (path_occupied) {
      return InvalidArgument(
          "Cannot create table '{}' at {}: path is occupied by a namespace.",
          identifier.ToString(), table_dir);
    }
    int8_t format_version = TableMetadata::kDefaultTableFormatVersion;
    for (const auto& update : updates) {
      if (update->kind() == TableUpdate::Kind::kUpgradeFormatVersion) {
        format_version =
            internal::checked_cast<const table::UpgradeFormatVersion&>(*update)
                .format_version();
      }
    }
    builder = TableMetadataBuilder::BuildFromEmpty(format_version);
    // Assign a fresh table UUID so the create-via-Transaction path is
    // consistent with CreateTable and the commit-time ABA recheck can
    // distinguish a drop+recreate generation by UUID.
    builder->AssignUUID();
  } else {
    std::string owner_id = name_ + ":" + identifier.ToString();
    ops.emplace(file_io_, table_dir, lock_manager_, std::move(owner_id));
    ICEBERG_ASSIGN_OR_RAISE(base, ops->Refresh());
    builder = TableMetadataBuilder::BuildFrom(base.get());
    // Tell the builder where the current metadata lives so its Build() can
    // append the corresponding MetadataLogEntry to the new metadata's
    // `metadata_log`. Without this, every Update commit overwrites the
    // history pointer and external readers see no metadata-log trail,
    // breaking the on-disk byte-for-byte parity HadoopCatalog claims with
    // Java. (The is_create path above starts from BuildFromEmpty so there
    // is no previous location to set; the v1 publish has metadata_log = [].)
    builder->SetPreviousMetadataLocation(ops->current_metadata_location());
  }

  for (const auto& requirement : requirements) {
    ICEBERG_RETURN_UNEXPECTED(requirement->Validate(base.get()));
  }
  for (const auto& update : updates) {
    update->ApplyTo(*builder);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto updated, builder->Build());

  if (is_create) {
    // Route through the same lock+UUID-temp+atomic-rename sequence as
    // CreateTable so create-via-Transaction shares CreateTable's safety.
    ICEBERG_ASSIGN_OR_RAISE(
        auto metadata_path,
        WriteInitialTableLocked(table_dir, *updated, "create:" + identifier.ToString()));
    std::shared_ptr<TableMetadata> updated_ptr = std::move(updated);
    return Table::Make(identifier, updated_ptr, metadata_path, file_io_,
                       shared_from_this());
  }

  ICEBERG_RETURN_UNEXPECTED(ops->Commit(*base, *updated));
  std::shared_ptr<TableMetadata> updated_ptr = std::move(updated);
  return Table::Make(identifier, updated_ptr, ops->current_metadata_location(), file_io_,
                     shared_from_this());
}

Result<std::shared_ptr<Transaction>> HadoopCatalog::StageCreateTable(
    const TableIdentifier& /*identifier*/, const std::shared_ptr<Schema>& /*schema*/,
    const std::shared_ptr<PartitionSpec>& /*spec*/,
    const std::shared_ptr<SortOrder>& /*order*/, const std::string& /*location*/,
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotSupported("HadoopCatalog::StageCreateTable is not yet supported.");
}

Result<bool> HadoopCatalog::TableExists(const TableIdentifier& identifier) const {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse_, identifier));
  auto result = hadoop::IsHadoopTableDir(*file_io_, table_dir);
  if (!result.has_value()) {
    if (SuppressedPermissionError(config_, result.error())) {
      return false;
    }
    return std::unexpected<Error>(result.error());
  }
  return *result;
}

Status HadoopCatalog::DropTable(const TableIdentifier& identifier, bool purge) {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse_, identifier));

  ICEBERG_ASSIGN_OR_RAISE(auto is_table, hadoop::IsHadoopTableDir(*file_io_, table_dir));
  if (!is_table) {
    return NoSuchTable("Table '{}' does not exist at {}.", identifier.ToString(),
                       table_dir);
  }
  if (!purge) {
    // HadoopCatalog can't honour the Catalog::DropTable purge=false contract
    // safely: the catalog identity of a path is the metadata/ subdirectory,
    // so "unregister but keep data" leaves the table dir with only `data/`,
    // which our namespace detector then exposes as a namespace -- a
    // subsequent CreateTable at the same path would silently adopt the
    // stranded data, and a later purge=true would destroy it. Java's
    // HadoopCatalog avoids the trap by ignoring purge=false (always
    // recursive-deletes), but that violates the documented contract on
    // our side. Refusing is the only safe option without adding a
    // tombstone format to the on-disk layout.
    return NotSupported(
        "HadoopCatalog::DropTable(purge=false) is not supported: the table "
        "directory IS the catalog entry, so 'unregister but keep data' would "
        "leave an unreachable subtree that subsequent CreateTable / "
        "ListNamespaces calls cannot distinguish from a real namespace. Use "
        "purge=true, or copy the data files out before dropping.");
  }
  // purge=true contract: delete ALL data and metadata files. Without an
  // Avro/manifest reader (lives in iceberg_bundle), iceberg_hadoop cannot
  // walk manifests to enumerate data files which Iceberg's write API can
  // place outside the table directory. We refuse purge in any state that
  // could leave external files behind, and we do the verification under
  // the same table lock as Commit so a concurrent writer cannot land a
  // new snapshot between our check and the delete.
  const std::string owner =
      name_ + ":drop:" + identifier.ToString() + ":" + Uuid::GenerateV7().ToString();
  ICEBERG_ASSIGN_OR_RAISE(auto acquired, lock_manager_->Acquire(table_dir, owner));
  if (!acquired) {
    return CommitFailed(
        "HadoopCatalog::DropTable: failed to acquire table lock for '{}' within "
        "the configured timeout; a concurrent writer is committing.",
        table_dir);
  }
  hadoop::LockReleaseGuard guard(lock_manager_.get(), table_dir, owner);

  hadoop::HadoopTableOperations ops(file_io_, table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, ops.Refresh());
  if (!metadata->snapshots.empty()) {
    return NotSupported(
        "HadoopCatalog::DropTable(purge=true) on '{}' is not supported: the table "
        "has {} snapshot(s) and the lightweight iceberg_hadoop library cannot "
        "walk manifests to enumerate data files (which Iceberg's write API may "
        "have placed outside the table directory). Drop via a catalog that "
        "supports manifest GC, or run an external manifest-walk to delete "
        "referenced data files first. NOTE: ExpireSnapshots alone is not enough "
        "-- it cannot expire the current snapshot, and the current snapshot's "
        "data files are exactly what we need to enumerate.",
        identifier.ToString(), metadata->snapshots.size());
  }
  // Iceberg's UpdateStatistics API does not require a snapshot to exist
  // and accepts arbitrary paths, so a snapshot-less table can still
  // reference external `.puffin` / partition-statistics / metadata-log
  // files. Refuse if any such path is not a descendant of table_dir.
  // Use the component-aware hadoop::IsPathInsideNormalized so that a sibling like
  // `/wh/db/stats_backup` cannot pass as inside `/wh/db/stats`.
  auto is_external = [&table_dir](std::string_view path) {
    return !hadoop::IsPathInsideNormalized(path, table_dir);
  };
  for (const auto& stat : metadata->statistics) {
    if (stat && is_external(stat->path)) {
      return NotSupported(
          "HadoopCatalog::DropTable(purge=true) on '{}' refused: statistics file "
          "'{}' lies outside the table directory '{}' and iceberg_hadoop cannot "
          "delete it. Remove or relocate the statistic, or drop via a catalog "
          "that owns the external location.",
          identifier.ToString(), stat->path, table_dir);
    }
  }
  for (const auto& stat : metadata->partition_statistics) {
    if (stat && is_external(stat->path)) {
      return NotSupported(
          "HadoopCatalog::DropTable(purge=true) on '{}' refused: partition "
          "statistics file '{}' lies outside the table directory '{}'.",
          identifier.ToString(), stat->path, table_dir);
    }
  }
  // metadata_log entries are historical metadata-file pointers. A registered
  // table could carry pointers outside table_dir (RegisterTable validates
  // them too, but a stale registration or external mutation could land an
  // outside reference). Refuse purge so we don't silently leave them behind.
  for (const auto& entry : metadata->metadata_log) {
    if (is_external(entry.metadata_file)) {
      return NotSupported(
          "HadoopCatalog::DropTable(purge=true) on '{}' refused: metadata-log "
          "entry '{}' lies outside the table directory '{}'.",
          identifier.ToString(), entry.metadata_file, table_dir);
    }
  }
  // No snapshots and no external statistic / metadata-log references:
  // every file we could be responsible for is under table_dir. Since
  // the lock file (for lock-impl=file) lives OUTSIDE table_dir (in
  // <warehouse>/_iceberg_catalog_locks/), we can simply recursive-
  // delete table_dir while still holding the lock -- no rmdir-after-
  // Release race with a concurrent CreateTable, since CreateTable
  // would block on Acquire until our Release.
  ICEBERG_RETURN_UNEXPECTED(file_io_->DeleteDir(table_dir, /*recursive=*/true));
  return {};
}

Status HadoopCatalog::RenameTable(const TableIdentifier& /*from*/,
                                  const TableIdentifier& /*to*/) {
  // Java HadoopCatalog explicitly throws UnsupportedOperationException: the
  // table identifier is derived from its path, so renaming would require
  // moving the on-disk data tree (not safe on object stores, and offered no
  // benefit on POSIX) -- callers should drop + recreate instead.
  return NotSupported(
      "HadoopCatalog::RenameTable is not supported; tables are path-identified, "
      "so rename would require moving on-disk data. Drop and recreate instead.");
}

Result<std::shared_ptr<Table>> HadoopCatalog::LoadTable(
    const TableIdentifier& identifier) {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse_, identifier));

  HadoopTableOperations ops(file_io_, table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, ops.Refresh());
  return Table::Make(identifier, std::move(metadata), ops.current_metadata_location(),
                     file_io_, shared_from_this());
}

Result<std::shared_ptr<Table>> HadoopCatalog::RegisterTable(
    const TableIdentifier& identifier, const std::string& metadata_file_location) {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  if (metadata_file_location.empty()) {
    return InvalidArgument(
        "HadoopCatalog::RegisterTable requires a non-empty metadata_file_location.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse_, identifier));

  ICEBERG_ASSIGN_OR_RAISE(auto already_table,
                          hadoop::IsHadoopTableDir(*file_io_, table_dir));
  if (already_table) {
    return AlreadyExists("Table '{}' already exists at {}.", identifier.ToString(),
                         table_dir);
  }
  ICEBERG_RETURN_UNEXPECTED(RejectTopLevelLockRoot(identifier.ns, identifier.name,
                                                   "HadoopCatalog::RegisterTable"));
  // Refuse to register a table inside an existing table -- a later
  // DropTable(purge=true) on the outer table would wipe the inner one.
  ICEBERG_RETURN_UNEXPECTED(hadoop::RejectAncestorIsTable(
      *file_io_, warehouse_, identifier.ns, /*last_inclusive=*/true));

  // Detect codec from the source filename so a gzipped source produces a
  // gzipped local copy. Accept BOTH the canonical core form
  // (`.gz.metadata.json`) and the legacy Hadoop form (`.metadata.json.gz`)
  // so an external Spark/Trino writer's output is registerable either way.
  const std::string_view file_name = hadoop::Basename(metadata_file_location);
  hadoop::MetadataCompressionCodec codec = hadoop::MetadataCompressionCodec::kNone;
  if (file_name.ends_with(".metadata.json.gz") ||
      file_name.ends_with(".gz.metadata.json")) {
    codec = hadoop::MetadataCompressionCodec::kGzip;
  } else if (file_name.ends_with(".metadata.json.zstd") ||
             file_name.ends_with(".zstd.metadata.json")) {
    codec = hadoop::MetadataCompressionCodec::kZstd;
  }

  // Read raw bytes once; parse + copy from the same buffer to avoid a
  // second IO round trip on large metadata files.
  ICEBERG_ASSIGN_OR_RAISE(auto raw,
                          file_io_->ReadFile(metadata_file_location, std::nullopt));
  std::string body = raw;
  if (codec == hadoop::MetadataCompressionCodec::kGzip) {
    GZipDecompressor decompressor;
    ICEBERG_RETURN_UNEXPECTED(decompressor.Init());
    ICEBERG_ASSIGN_OR_RAISE(body, decompressor.Decompress(raw));
  } else if (codec == hadoop::MetadataCompressionCodec::kZstd) {
    return NotSupported(
        "HadoopCatalog::RegisterTable: zstd-compressed metadata is not yet "
        "supported by iceberg-cpp.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(body));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, TableMetadataFromJson(json));

  // The metadata MUST carry a table-uuid. An empty uuid would defeat the
  // commit-time ABA guard (HadoopTableOperations::Commit step 4b): two
  // uuid-less generations at the same version compare equal, so a stale
  // update could clobber a concurrently recreated table. Refuse to import
  // uuid-less metadata up front.
  if (metadata->table_uuid.empty()) {
    return InvalidArgument(
        "HadoopCatalog::RegisterTable: source metadata at '{}' has no table-uuid; "
        "uuid-less tables cannot be guarded against concurrent drop+recreate.",
        metadata_file_location);
  }

  // Same Java parity rule HadoopCatalog::Commit / CreateTable enforce:
  // path-based tables cannot redirect their metadata directory via
  // `write.metadata.path`.
  if (metadata->properties.configs().contains(
          TableProperties::kWriteMetadataLocation.key())) {
    return InvalidArgument(
        "HadoopCatalog::RegisterTable: source metadata sets '{}', which Hadoop "
        "path-based tables do not support.",
        TableProperties::kWriteMetadataLocation.key());
  }
  // Data must stay under table_dir so DropTable(purge=true) can reach it.
  ICEBERG_RETURN_UNEXPECTED(RejectExternalDataPathProperty(
      metadata->properties.configs(), "HadoopCatalog::RegisterTable"));
  ICEBERG_RETURN_UNEXPECTED(RejectCustomLocation(table_dir, metadata->location,
                                                 "HadoopCatalog::RegisterTable"));
  // statistics / partition_statistics / metadata_log entries are external
  // file references that DropTable(purge=true) would have to clean up. We
  // can't, without a manifest walker, reach files outside table_dir on
  // drop -- so refuse the registration up front if any such reference
  // points outside, instead of silently producing a table that purge
  // cannot honour.
  for (const auto& stat : metadata->statistics) {
    if (stat && !hadoop::IsPathInsideNormalized(stat->path, table_dir)) {
      return InvalidArgument(
          "HadoopCatalog::RegisterTable: statistics file '{}' is outside the "
          "table dir '{}'; DropTable(purge=true) could not clean it.",
          stat->path, table_dir);
    }
  }
  for (const auto& stat : metadata->partition_statistics) {
    if (stat && !hadoop::IsPathInsideNormalized(stat->path, table_dir)) {
      return InvalidArgument(
          "HadoopCatalog::RegisterTable: partition statistics file '{}' is "
          "outside the table dir '{}'.",
          stat->path, table_dir);
    }
  }
  for (const auto& entry : metadata->metadata_log) {
    if (!hadoop::IsPathInsideNormalized(entry.metadata_file, table_dir)) {
      return InvalidArgument(
          "HadoopCatalog::RegisterTable: metadata-log entry '{}' is outside "
          "the table dir '{}'; DropTable(purge=true) could not clean it.",
          entry.metadata_file, table_dir);
    }
  }

  // Publish via the same lock + UUID-temp + atomic-rename path as CreateTable
  // so concurrent register/create attempts cannot overwrite each other.
  ICEBERG_ASSIGN_OR_RAISE(auto target,
                          WriteInitialBytesLocked(table_dir, raw, codec,
                                                  "register:" + identifier.ToString()));
  std::shared_ptr<TableMetadata> metadata_ptr = std::move(metadata);
  return Table::Make(identifier, metadata_ptr, target, file_io_, shared_from_this());
}

}  // namespace iceberg::hadoop
