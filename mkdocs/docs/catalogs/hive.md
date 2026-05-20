<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Hive Catalog

The `iceberg_hive` library exposes an Iceberg [`Catalog`](https://iceberg.apache.org/docs/latest/api/) implementation backed by a Hive Metastore (HMS). It is a port of `iceberg-rust`'s `iceberg-catalog-hms` crate with the addition of a Java-style commit / CAS path landing in Phase 2.

## Status

| Capability | Phase 1 | Phase 2 |
|---|:-:|:-:|
| Namespace CRUD (create / list / properties / update / drop / exists) | ✅ | ✅ |
| Table list / drop / rename / exists | ✅ | ✅ |
| `LoadTable` (read existing metadata.json) | ✅ | ✅ |
| `RegisterTable` (attach an existing metadata.json) | ✅ | ✅ |
| `CreateTable` (write a fresh metadata.json) | ❌ (depends on `TableMetadataToJson`) | ✅ |
| `UpdateTable` (commit with `metadata_location` CAS) | ❌ | ✅ |
| Optional HMS `lock` / `unlock` around commit | ❌ | ✅ |
| SASL / Kerberos authentication | ❌ | Phase 3 |

## Build

`iceberg_hive` is an opt-in library; it is not built by default to keep the core library free of Apache Thrift.

```bash
brew install thrift                # macOS
# or: apt install thrift-compiler libthrift-dev   on Debian/Ubuntu

cmake -S . -B build -G Ninja \
  -DICEBERG_BUILD_HIVE=ON
cmake --build build
```

Two additional CMake options are available:

| Option | Default | Description |
|---|---|---|
| `ICEBERG_BUILD_HIVE` | `OFF` | Build the `iceberg_hive` library and unit tests |
| `ICEBERG_BUILD_HIVE_INTEGRATION_TESTS` | `OFF` | Also build the docker-compose-backed integration test binary (requires `apache/hive:4.0.0` + MinIO via `docker compose`) |

The integration tests live under `src/iceberg/test/resources/iceberg-hive-fixture/` and are not enabled in the default CI matrix.

## Linking

`iceberg_hive` is published as a separate library target alongside `iceberg` and `iceberg_rest`:

```cmake
find_package(Iceberg REQUIRED)
target_link_libraries(my_app PRIVATE iceberg::iceberg_hive)
```

## Usage

```cpp
#include "iceberg/catalog/hive/hive_catalog.h"
#include "iceberg/catalog/hive/hive_catalog_properties.h"

using iceberg::hive::HiveCatalog;
using iceberg::hive::HiveCatalogProperties;

auto config = HiveCatalogProperties::FromMap({
    {std::string(HiveCatalogProperties::kUri.key()),
     "thrift://hms.example.com:9083"},
    {std::string(HiveCatalogProperties::kWarehouse.key()),
     "s3://my-bucket/warehouse"},
    {std::string(HiveCatalogProperties::kName.key()), "prod"},
});

auto catalog_result = HiveCatalog::Make(config);
if (!catalog_result) {
  // catalog_result.error() carries the typed iceberg::Error.
  return catalog_result.error();
}
auto catalog = std::move(*catalog_result);

(void)catalog->CreateNamespace(iceberg::Namespace{.levels = {"warehouse"}},
                               {{"owner", "data-platform"}});
```

## Configuration properties

| Key | Default | Description |
|---|---|---|
| `uri` | (required) | HMS Thrift endpoint(s). `thrift://host:port`, bare `host:port`, or comma-separated for HA failover. |
| `name` | `hive` | Catalog name returned by `Catalog::name()`. |
| `warehouse` | empty | Filesystem root for new tables. Also used by `LoadTable` to auto-detect the FileIO backend (`file://` → `arrow-fs-local`, `s3://` → `arrow-fs-s3`). |
| `io-impl` | derived from `warehouse` | Explicit FileIO implementation name (`arrow-fs-local`, `arrow-fs-s3`, …). |
| `thrift-transport` | `buffered` | `buffered` or `framed`. Match what your HMS deployment expects. |
| `connect-timeout-ms` | `30000` | HMS connect timeout. |
| `socket-timeout-ms` | `60000` | HMS read / write timeout. |
| `hive.lock-enabled` | `false` | When Phase 2 lands, wraps the commit path with HMS `lock` / `unlock` for additional concurrency safety on top of `metadata_location` CAS. |

## Thrift IDL provenance

The HMS Thrift bindings are generated at build time from `third_party/hive_metastore/hive_metastore.thrift`, an Apache 2.0–licensed copy of Hive 4.0's `standalone-metastore-common/.../hive_metastore.thrift` (see `third_party/hive_metastore/NOTICE`). The `iceberg_hive_thrift_bindings` CMake target exposes the codegen step in isolation if you want to inspect or regenerate the outputs locally.

## Interoperability

Iceberg tables created (or registered) through `iceberg_hive` carry the standard Iceberg-on-HMS marker table parameters (`table_type=ICEBERG`, `EXTERNAL=TRUE`, `metadata_location`, `LazySimpleSerDe`, `FileInputFormat`, `FileOutputFormat`). This keeps them readable by Spark, Trino and iceberg-java's Hive integration without any further configuration.
