<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Hadoop Catalog

`HadoopCatalog` is the filesystem-backed catalog. Tables live directly under a
warehouse directory; there is no metastore and no HTTP service to operate.
Commit-time consistency is achieved by an atomic
`Rename(overwrite=false)` of the new `v{N+1}.metadata.json` file (created
under a UUID-named temp) as the CAS primitive, plus an in-process or
filesystem lock; `version-hint.text` is then published via an
atomic-replace `Rename(overwrite=true)`. The on-disk layout and commit
protocol are byte-for-byte compatible with Apache Iceberg's Java
`HadoopCatalog`.

## When to use it

| You want… | Use |
|---|---|
| Local development / CI / one-process ETL | `HadoopCatalog` on `file://` |
| HDFS clusters that already expose `version-hint.text` | `HadoopCatalog` on `hdfs://` (see [Kerberos / HDFS](#hdfs-and-kerberos)) |
| Multi-tenant shared catalog | `RestCatalog` or `HiveCatalog` |
| Object storage (S3) at scale | Prefer `RestCatalog` or `HiveCatalog`; HadoopCatalog on S3 is supported but emits a runtime warning because S3 has no atomic rename. |

## Build

The library is enabled by default. Toggle with the CMake option
`ICEBERG_BUILD_HADOOP` (or the Meson feature option `hadoop`):

```bash
cmake -B build -DICEBERG_BUILD_HADOOP=ON
cmake --build build
```

To consume from a downstream CMake project:

```cmake
find_package(iceberg CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE iceberg::iceberg_hadoop)
```

If your code uses `HadoopCatalog::Make(name, config)` (the auto-detect
overload that picks the FileIO from the warehouse scheme), you also need
to link `iceberg::iceberg_bundle`. The arrow-backed FileIOs
(`arrow-fs-local`, `arrow-fs-s3`, `arrow-fs-hdfs`) are registered by the
bundle's static initialiser; without that link the auto-detect Make
returns `kInvalidArgument` with a message that says exactly this.
Alternatives:

```cmake
# Option A: pull in the bundle (registers all FileIOs at static-init).
target_link_libraries(my_app PRIVATE iceberg::iceberg_hadoop iceberg::iceberg_bundle)
```

```cpp
// Option B: register explicitly before the catalog is made.
#include "iceberg/arrow/arrow_io_register.h"
iceberg::arrow::EnsureArrowFileIOsRegistered();
auto catalog = HadoopCatalog::Make("...", props).value();
```

```cpp
// Option C: pass an explicit FileIO (no bundle dependency required).
auto catalog = HadoopCatalog::Make("...", file_io, props).value();
```

## Quick start

```cpp
#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/catalog/hadoop/hadoop_catalog.h"
#include "iceberg/catalog/hadoop/hadoop_catalog_properties.h"

using iceberg::Namespace;
using iceberg::TableIdentifier;
using iceberg::hadoop::HadoopCatalog;
using iceberg::hadoop::HadoopCatalogProperties;

auto props = HadoopCatalogProperties::FromMap({
    {"warehouse", "file:///tmp/iceberg-wh"},
    {"name", "my_catalog"},
});

// arrow-fs-local works out of the box for file:// warehouses.
auto file_io = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(
    std::make_shared<arrow::fs::LocalFileSystem>());

auto catalog = HadoopCatalog::Make("my_catalog", file_io, std::move(props)).value();

catalog->CreateNamespace(Namespace{.levels = {"db"}}, {});
auto table = catalog->CreateTable(
    TableIdentifier{.ns = {.levels = {"db"}}, .name = "events"},
    schema, iceberg::PartitionSpec::Unpartitioned(),
    iceberg::SortOrder::Unsorted(),
    /*location=*/"", /*properties=*/{}).value();
```

## On-disk layout

```
<warehouse>/
  <ns_level_1>/.../<ns_level_n>/
    <table>/
      metadata/
        v1.metadata.json[.gz|.zstd]
        v2.metadata.json[.gz|.zstd]
        ...
        version-hint.text          # plain integer pointing at the current version
        _lock                      # optional, written by FileLockManager
      data/                        # writer-managed
```

`version-hint.text` is updated by writing a UUID-suffixed temp file
(`version-hint.text.tmp.<uuid>`) and then issuing an atomic
`Rename(overwrite=true)`. The commit-time lock serialises writers; the
atomic replace keeps the protocol crash-safe (there is no window during
which the hint file is absent). If the hint is missing or corrupt, readers
fall back to a directory scan that returns the maximum `vN` file present.

## Properties

| Key | Default | Notes |
|---|---|---|
| `warehouse` | (required) | Base directory for the catalog. `file://`, `hdfs://`, and `s3://` are recognised; `s3://` emits a warning. |
| `io-impl` | (auto-detect) | FileIO implementation override. Empty means infer from the warehouse scheme. |
| `name` | `""` | Catalog name; used in log lines and lock owner ids. |
| `cache-enabled` / `cache.case-sensitive` / `cache.expiration-interval-ms` | true / true / 30000 | Java config parity. The cpp catalog does not maintain a metadata cache at this layer; the keys are accepted to keep a shared config file portable. |
| `io.manifest.cache-*` | (defaults) | Manifest cache config, parsed but not honoured (FileIO layer concern). |
| `lock-impl` | `in-memory` | `in-memory` (default) or `file`. Other names return `kInvalidArgument`. |
| `lock.acquire-interval-ms` | 5000 | Probe interval while waiting for the lock. |
| `lock.acquire-timeout-ms` | 180000 | Lock acquisition timeout (3 min). |
| `lock.heartbeat-interval-ms` / `lock.heartbeat-timeout-ms` | 3000 / 15000 | Honored by `FileLockManager`; no-op for `InMemoryLockManager`. |
| `lock.heartbeat-threads` | 4 | Accepted for Java config compatibility; the cpp `FileLockManager` runs one heartbeat thread per held lock and ignores this value. |
| `suppress-permission-error` | `false` | When true, downgrade permission errors on list / exists paths (including Arrow's `kIOError` wrapping `EACCES`/`EPERM`) into warnings instead of returning the error. |
| `fs.defaultFS` / `hadoop.security.authentication` / `hadoop.kerberos.principal` / `hadoop.kerberos.keytab` | (empty) | Forwarded to the HDFS FileIO `extra_conf` when the warehouse scheme is `hdfs://`. |

Any property whose key begins with `hadoop.`, `dfs.`, or `fs.` is preserved
verbatim and passed through to the underlying HDFS FileIO when applicable.

## Commit protocol

`HadoopTableOperations::Commit` follows the same 10-step protocol as Java's
implementation:

1. Reject any change to `metadata.location` (path-based tables cannot
   relocate). Returns `kInvalidArgument`.
2. Reject metadata whose properties contain `write.metadata.location`.
3. Acquire the `LockManager` for `<warehouse>/<ns…>/<table>` with the
   configured `lock.acquire-timeout-ms`. Timeout → `kCommitFailed`.
4. Re-resolve `version-hint.text`. If it has advanced past `base`'s version
   → `kCommitFailed` (so `iceberg::Transaction` can retry).
5. Pick the target filename: `v{N+1}.metadata.json` plus the codec suffix
   selected by `write.metadata.compression-codec` (`none` / `gzip`).
6. Serialise + (optionally) compress the new metadata into a UUID-named
   temp file under `metadata/`. Crashed writers leave UUID-named orphans
   that listdir-based recovery ignores.
7. Atomically rename the temp file to `v{N+1}.metadata.json[.codec]` with
   `overwrite=false`. This is the CAS primitive: if another writer beat
   us to `v{N+1}`, the rename fails and we surface `kCommitFailed`.
8. Update `version-hint.text` via write-tmp + `Rename(overwrite=true)`.
   The lock already serialises writers, so the atomic replace is enough;
   no delete-then-rename window where the hint is absent.
9. If the hint rename fails, re-read the metadata directory. If the hint
   has somehow already advanced to our version, treat as success.
   Otherwise clean up the v{N+1} file + tmp hint and return
   `kCommitFailed`.
10. Release the lock before any commit-time GC (`PruneOldMetadataFiles`)
    so long delete loops on deep history do not stall other writers.

## Cross-process safety envelope

The commit protocol's CAS depends on `Rename(overwrite=false)` being a true
atomic create-if-absent. In iceberg-cpp:

- **`file://` (LocalFileSystem)** — safe. Implemented via POSIX `link(2)`
  through `std::filesystem::create_hard_link`; two competing writers see
  `kAlreadyExists` deterministically.
- **`hdfs://`, `s3://` / `s3a://` / `s3n://`, `mockfs://`** — **NOT safe
  cross-process**. arrow's `Move` on these backends does not expose an
  atomic create-if-absent primitive, so iceberg-cpp falls back to a
  `GetFileInfo` precheck + unconditional `Move`. The precheck has a
  TOCTOU window: two concurrent commits can both observe "absent" and
  both rename, overwriting each other's metadata. `HadoopCatalog::Make`
  emits a runtime warning for these warehouses.

For production deployments on those backends use `lock-impl=in-memory`
when a single process is the only writer, or layer an external
coordination service (DynamoDB lock manager, S3 conditional PUT, etc.)
on top -- iceberg-cpp does not wire one up automatically.

**Stale-lock reclamation is not performed inline by `Acquire`.** A
`read body -> rename source aside` snatch is unsafe without a "verify
body THEN unlink source" primitive (it can race a third party that just
cleaned the stale lock and published a fresh one). `Acquire` instead
leaves the stale `_lock` file in place and emits a one-time-per-call
warning identifying the recorded owner. Operators (or an external
reaper that knows the holder is truly gone) must remove the file with
something like `hdfs dfs -rm` / `aws s3 rm` / `rm` for the local case;
the next `Acquire` then succeeds via the normal rename CAS.

**Heartbeat refresh is best-effort even on LocalFileSystem.** The
background heartbeat thread does a read-check-write to refresh the lock
timestamp without an atomic compare-and-write primitive available in
`FileIO`. If the on-disk lock is externally reaped and another acquirer
publishes a fresh body between our heartbeat read and write, our stale
write can clobber the new body. The race window is bounded
(microseconds), consequences are bounded too: the stealer's later
`Release` returns `kNotAllowed`, and HadoopCatalog's commit-time CAS on
`v{N+1}.metadata.json` prevents any actual data corruption. The lock
body is informational; the real safety is the Acquire-time rename CAS
plus the commit-time metadata rename CAS.

## HDFS and Kerberos

HDFS support is enabled by `ICEBERG_HDFS=ON` (off by default because it
requires a JVM + Hadoop client at runtime). When enabled, the FileIO factory
recognises `hdfs://` warehouses and constructs an `arrow-fs-hdfs` FileIO.
HadoopCatalog itself contains **no Kerberos code**: authentication is handled
entirely by libhdfs, which inherits the JVM's UGI. Operationally:

```bash
export JAVA_HOME=/path/to/jdk
export HADOOP_HOME=/path/to/hadoop
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob)

kinit -kt /etc/keytabs/svc.keytab svc/_HOST@REALM
# Now your process can construct HadoopCatalog with warehouse=hdfs://nn:8020/wh
```

The `hadoop.kerberos.principal` / `hadoop.kerberos.keytab` properties are
preserved when forwarding configuration to the FileIO, but the actual TGT
acquisition is performed externally by `kinit` (or by the JVM via
`-Djava.security.auth.login.config`). This matches what
`org.apache.hadoop.security.UserGroupInformation` does on the Java side.

## What HadoopCatalog does not do

To stay byte-for-byte compatible with Java, the cpp implementation rejects the
same operations:

| Operation | Behaviour |
|---|---|
| `CreateNamespace(ns, non_empty_properties)` | `kNotSupported` (Java throws `UnsupportedOperationException`). |
| `UpdateNamespaceProperties` | `kNotSupported`. |
| `RenameTable` | `kNotSupported`. Renaming a filesystem subtree across the catalog is non-atomic and Java disables it for the same reason. |
| `loadNamespaceMetadata` | Returns `{"location": <ns_path>}` only. |

## Status matrix vs Java

| Feature | Java | iceberg-cpp |
|---|---|---|
| Namespace CRUD + filtering of table dirs | ✅ | ✅ |
| Table CRUD on local / HDFS | ✅ | ✅ |
| Commit + LockManager + CAS recovery | ✅ | ✅ |
| `InMemoryLockManager` default | ✅ | ✅ |
| `FileLockManager` (`lock-impl=file`) | ❌ (plugin) | ✅ on `file://`; ⚠ best-effort only on `hdfs://`/`s3://`/`mockfs://` (see Cross-process safety envelope) |
| `write.metadata.compression-codec=gzip` (on commit AND initial create) | ✅ | ✅ |
| `write.metadata.compression-codec=zstd` | ✅ | ❌ (`kNotSupported`; iceberg-cpp metadata reader has no zstd path) |
| `write.metadata.previous-versions-max` / `write.metadata.delete-after-commit.enabled` | ✅ | ✅ |
| `DropTable(purge=true)` snapshot data cleanup via manifest walk | ✅ | ⚠ recursive directory delete only; `LogWarning` flags the divergence |
| `suppress-permission-error` | ✅ | ✅ (covers `kForbidden`, `kNotAuthorized`, and arrow `kIOError` with permission messages) |
| `HadoopTables` single-table API | ✅ | ✅ |
| HDFS via Arrow `HadoopFileSystem` (libhdfs JNI) | n/a (Java is native) | ✅ (build with `ICEBERG_HDFS=ON`) |
| Kerberos passthrough via JVM UGI | ✅ | ✅ (see HDFS section above) |
| `RenameTable` | ❌ (NotSupported) | ❌ (NotSupported) |
| `CreateNamespace` with non-empty properties | ❌ (Unsupported) | ❌ (NotSupported) |
