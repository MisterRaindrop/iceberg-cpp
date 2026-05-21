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
Commit-time consistency is achieved by an atomic rename of `version-hint.text`
plus an in-process or filesystem lock. The on-disk layout and commit protocol
are byte-for-byte compatible with Apache Iceberg's Java `HadoopCatalog`.

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

`version-hint.text` is updated atomically by writing
`version-hint.text.tmp`, deleting the old hint, and renaming the temp file in
place. If the hint is missing or corrupt, readers fall back to a directory scan
that returns the maximum `vN` file present.

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
| `lock.heartbeat-interval-ms` / `lock.heartbeat-timeout-ms` / `lock.heartbeat-threads` | 3000 / 15000 / 4 | Honored by `FileLockManager` (H14); no-op for `InMemoryLockManager`. |
| `suppress-permission-error` | `false` | When true, downgrade `kForbidden` errors on list / exists into warnings (introduced alongside `H17`). |
| `fs.defaultFS` / `hadoop.security.authentication` / `hadoop.kerberos.principal` / `hadoop.kerberos.keytab` | (empty) | Forwarded to the HDFS FileIO `extra_conf` when the warehouse scheme is `hdfs://` (H21). |

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
   selected by `write.metadata.compression-codec` (codec support arrives in
   H15).
6. Refuse to overwrite an existing file at that path → `kCommitFailed`.
7. Write the new metadata.
8. Three-step pointer update: write `version-hint.text.tmp`, delete the old
   `version-hint.text`, rename `.tmp` → `version-hint.text` without
   overwrite. The rename is the CAS primitive.
9. If rename fails, re-read the hint. If somebody else (or a recovered
   crash) already advanced it to our new version, treat it as success.
   Otherwise clean up the new file and `.tmp`, return `kCommitFailed`.
10. Release the lock in a finally-like cleanup.

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
| Table CRUD on local / HDFS | ✅ | ✅ (HDFS wiring via H20–H21) |
| Commit + LockManager + CAS recovery | ✅ | ✅ |
| `InMemoryLockManager` default | ✅ | ✅ |
| `FileLockManager` (`lock-impl=file`) | ❌ (plugin) | ⏳ H14 |
| `write.metadata.compression-codec` | ✅ | ⏳ H15 |
| `commit.metadata.previous-versions-max` / `delete-after-commit.enabled` | ✅ | ⏳ H16 |
| `DropTable(purge=true)` snapshot data cleanup | ✅ | ⏳ H17 |
| `suppress-permission-error` | ✅ | ⏳ H17 |
| `HadoopTables` single-table API | ✅ | ⏳ H19 |
| HDFS via Arrow `HadoopFileSystem` (libhdfs JNI) | n/a (Java is native) | ⏳ H20 |
| Kerberos passthrough via JVM UGI | ✅ | ⏳ H21 (docs above) |
| `RenameTable` | ❌ (NotSupported) | ❌ (NotSupported) |
| `CreateNamespace` with non-empty properties | ❌ (Unsupported) | ❌ (NotSupported) |
