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

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"

#include <array>
#include <format>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg::hadoop {

namespace {

constexpr std::string_view kJsonSuffix = ".metadata.json";
constexpr std::string_view kVersionPrefix = "v";

std::string JoinUnderRoot(std::string_view root, std::string_view child) {
  std::string base{LocationUtil::StripTrailingSlash(root)};
  base.push_back('/');
  base.append(child);
  return base;
}

// Forward declarations -- defined further down (PercentDecode and
// LooksLikeUri live near IsPathInsideNormalized). CanonicalizeWarehouse
// needs them at top-of-file.
bool PercentDecode(std::string_view s, std::string& out);
bool LooksLikeUri(std::string_view path);

}  // namespace

Result<std::string> CanonicalizeWarehouse(std::string_view warehouse) {
  // IMPORTANT: do NOT percent-decode here. The result is stored as the
  // warehouse prefix and re-joined into table / namespace URIs that the
  // FileIO layer re-parses via arrow's PathFromUri. Decoding a URI and
  // feeding the decoded string back as a URI is lossy and dangerous:
  //   - `file:///tmp/a%23b` (dir `/tmp/a#b`) would decode to
  //     `file:///tmp/a#b`; appending `/db` then makes `#b/db` a URI
  //     fragment and the IO target collapses to `/tmp/a`.
  //   - `%252F` would decode to `%2F` and then to `/` on the second
  //     parse, escaping into a path separator.
  // So the only safe normalisation for the IO-facing warehouse string is
  // to strip a trailing slash (which never changes the resolved path).
  // Alias collapsing for LOCK identity is handled separately by
  // CanonicalLockKey, which produces a key that is never used for IO.
  std::string canonical(warehouse);
  while (canonical.size() > 1 && canonical.back() == '/') {
    canonical.pop_back();
  }
  return canonical;
}

std::string CanonicalLockKey(std::string_view entity_id) {
  // Produce a STABLE physical-identity string for use ONLY as a lock map
  // key / file-lock hash input. Unlike the warehouse prefix, this is
  // never re-parsed as an IO URI, so we are free to fully normalise it:
  //   1. percent-decode the URI form (so `%20` and a literal space, or
  //      `%2F` and `/`, map to one key);
  //   2. collapse `.` and `..` path segments (so `file:///tmp/wh` and
  //      `file:///tmp/./wh` share a key);
  //   3. strip a trailing slash.
  // Literal local paths are not percent-decoded (a `%` there is a real
  // filename byte) but their `.`/`..` segments are still collapsed.
  std::string decoded;
  if (LooksLikeUri(entity_id)) {
    if (!PercentDecode(entity_id, decoded)) {
      decoded.assign(entity_id);  // malformed -- fall back to raw bytes
    }
  } else {
    decoded.assign(entity_id);
  }
  // Collapse `.` / `..` segments. We treat both `/` and the run of
  // leading separators (`scheme://`) conservatively: empty segments are
  // preserved so `file://` authority structure is not mangled; only
  // explicit `.` and `..` non-empty segments are folded.
  std::vector<std::string_view> out;
  size_t i = 0;
  const std::string_view view = decoded;
  while (i <= view.size()) {
    size_t next = view.find('/', i);
    const std::string_view seg =
        next == std::string_view::npos ? view.substr(i) : view.substr(i, next - i);
    if (seg == ".") {
      // drop
    } else if (seg == "..") {
      // Pop the previous non-empty, non-"" segment if there is one.
      if (!out.empty() && !out.back().empty() && out.back() != "..") {
        out.pop_back();
      } else {
        out.push_back(seg);
      }
    } else {
      out.push_back(seg);
    }
    if (next == std::string_view::npos) {
      break;
    }
    i = next + 1;
  }
  std::string joined;
  for (size_t k = 0; k < out.size(); ++k) {
    if (k > 0) {
      joined.push_back('/');
    }
    joined.append(out[k]);
  }
  while (joined.size() > 1 && joined.back() == '/') {
    joined.pop_back();
  }
  return joined;
}

bool IsS3Scheme(std::string_view location) {
  return location.starts_with("s3://") || location.starts_with("s3a://") ||
         location.starts_with("s3n://");
}

std::string_view Basename(std::string_view path) {
  auto slash = path.find_last_of('/');
  return slash == std::string_view::npos ? path : path.substr(slash + 1);
}

Result<MetadataCompressionCodec> ParseMetadataCompressionCodec(std::string_view name) {
  const std::string lower = StringUtils::ToLower(name);
  if (lower.empty() || lower == "none") {
    return MetadataCompressionCodec::kNone;
  }
  if (lower == "gzip") {
    return MetadataCompressionCodec::kGzip;
  }
  if (lower == "zstd") {
    return MetadataCompressionCodec::kZstd;
  }
  return InvalidArgument(
      "Unknown metadata compression codec '{}'; expected 'none', 'gzip', or 'zstd'.",
      name);
}

std::string_view MetadataCompressionCodecName(MetadataCompressionCodec codec) {
  switch (codec) {
    case MetadataCompressionCodec::kNone:
      return "none";
    case MetadataCompressionCodec::kGzip:
      return "gzip";
    case MetadataCompressionCodec::kZstd:
      return "zstd";
  }
  return "none";
}

namespace {

// Names reserved by the HadoopCatalog on-disk layout. Allowing a namespace
// level or table name to collide with one of these would let a caller bury
// existing tables: e.g. CreateNamespace(db.team.metadata) + CreateTable
// nested under it makes the catalog point at the wrong directory the moment
// someone calls CreateTable(db.team) (whose `metadata/` subdir would happen
// to be the existing namespace).
constexpr std::array<std::string_view, 3> kReservedComponentNames = {"metadata", "data",
                                                                     kLockRootDirName};

bool IsReservedComponent(std::string_view component) {
  for (const auto& reserved : kReservedComponentNames) {
    if (component == reserved) {
      return true;
    }
  }
  return false;
}

}  // namespace

namespace {

// Identifier components are joined raw into a URI and later decoded by
// arrow's URI parser. A `%` in the component would be parsed as the start
// of a percent-encoded sequence -- e.g. `%2e%2e` decodes to `..` and
// escapes the warehouse. We do not need percent-encoding for catalog
// identifiers (Iceberg does not), so reject the marker outright. Same
// rationale for `\\` (Windows path separator) and ASCII control bytes
// (NUL would truncate a path; 0x01-0x1f and 0x7f have no legitimate use
// in identifiers). Bytes 0x80+ are kept allowed so legitimate UTF-8
// names like `客户.订单` continue to load -- the percent-encoding gate
// above already blocks the traversal vector and Java HadoopCatalog
// accepts Unicode identifiers.
Status RejectUnsafeIdentifierChars(std::string_view component, std::string_view kind) {
  for (unsigned char c : component) {
    if (c < 0x20 || c == 0x7f) {
      return InvalidArgument(
          "{} '{}' contains a control byte (0x{:02x}); control bytes are not "
          "allowed in identifiers.",
          kind, component, static_cast<int>(c));
    }
    if (c == '%') {
      return InvalidArgument(
          "{} '{}' contains '%' which is the URI percent-encoding marker "
          "and could be decoded into path-traversal sequences "
          "(e.g. %2e%2e -> '..'); reject up front.",
          kind, component);
    }
    if (c == '\\') {
      return InvalidArgument(
          "{} '{}' contains '\\' which is the Windows path separator; "
          "use '/' (forbidden separately) or rename.",
          kind, component);
    }
    if (c == '?' || c == '#') {
      // URI reserved separators (`?` starts the query, `#` the fragment).
      // arrow's URI parser splits at them, so a name like `db?use_mmap`
      // would resolve to the SAME physical directory as `db` -- letting
      // a caller delete or overwrite another logical identifier's data.
      // Reject these characters at the validator.
      return InvalidArgument(
          "{} '{}' contains '{}' which is a URI reserved separator and would "
          "be stripped from the path at IO time, aliasing two distinct "
          "identifiers to the same directory.",
          kind, component, static_cast<char>(c));
    }
  }
  return {};
}

}  // namespace

Status ValidateNamespaceLevel(std::string_view level) {
  if (level.empty()) {
    return InvalidArgument("Namespace level must not be empty.");
  }
  if (level.find('/') != std::string_view::npos) {
    return InvalidArgument("Namespace level '{}' must not contain '/'.", level);
  }
  // POSIX path component traversal: `.` aliases the parent dir (so it would
  // overlay an existing namespace with table metadata), and `..` escapes
  // upward (allowing reads/writes/deletes outside the warehouse). Both must
  // be rejected before the component is joined into a path.
  if (level == "." || level == "..") {
    return InvalidArgument(
        "Namespace level '{}' is a path-traversal alias; '.' and '..' are not "
        "allowed in HadoopCatalog identifiers.",
        level);
  }
  ICEBERG_RETURN_UNEXPECTED(RejectUnsafeIdentifierChars(level, "Namespace level"));
  if (IsReservedComponent(level)) {
    return InvalidArgument(
        "Namespace level '{}' is reserved by the HadoopCatalog on-disk layout "
        "(table-internal subdirectory name); choose a different name.",
        level);
  }
  return {};
}

Status ValidateNamespace(const Namespace& ns) {
  for (const auto& level : ns.levels) {
    ICEBERG_RETURN_UNEXPECTED(ValidateNamespaceLevel(level));
  }
  return {};
}

Status ValidateTableIdentifier(const TableIdentifier& identifier) {
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());
  if (identifier.name.find('/') != std::string::npos) {
    return InvalidArgument("Table name '{}' must not contain '/'.", identifier.name);
  }
  // Same path-traversal guard as ValidateNamespaceLevel: a table named "."
  // resolves to the parent namespace dir, and ".." escapes the warehouse.
  if (identifier.name == "." || identifier.name == "..") {
    return InvalidArgument(
        "Table name '{}' is a path-traversal alias; '.' and '..' are not "
        "allowed in HadoopCatalog identifiers.",
        identifier.name);
  }
  ICEBERG_RETURN_UNEXPECTED(RejectUnsafeIdentifierChars(identifier.name, "Table name"));
  if (IsReservedComponent(identifier.name)) {
    return InvalidArgument(
        "Table name '{}' is reserved by the HadoopCatalog on-disk layout "
        "(table-internal subdirectory name); choose a different name.",
        identifier.name);
  }
  return ValidateNamespace(identifier.ns);
}

Result<std::string> NamespaceDir(std::string_view warehouse, const Namespace& ns) {
  if (warehouse.empty()) {
    return InvalidArgument("warehouse must not be empty when computing namespace dir.");
  }
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));

  std::string out{LocationUtil::StripTrailingSlash(warehouse)};
  for (const auto& level : ns.levels) {
    out.push_back('/');
    out.append(level);
  }
  return out;
}

Result<std::string> TableDir(std::string_view warehouse,
                             const TableIdentifier& identifier) {
  ICEBERG_RETURN_UNEXPECTED(ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, NamespaceDir(warehouse, identifier.ns));
  return JoinUnderRoot(ns_dir, identifier.name);
}

std::string MetadataDir(std::string_view table_dir) {
  return JoinUnderRoot(table_dir, "metadata");
}

std::string DataDir(std::string_view table_dir) {
  return JoinUnderRoot(table_dir, "data");
}

std::string MetadataFileName(int64_t version, MetadataCompressionCodec codec) {
  switch (codec) {
    case MetadataCompressionCodec::kNone:
      return std::format("v{}.metadata.json", version);
    case MetadataCompressionCodec::kGzip:
      // Match TableMetadataUtil::Codec::kGzipTableMetadataFileSuffix
      // (".gz.metadata.json") -- the canonical form per the core library.
      // ParseMetadataFileName also accepts the legacy ".metadata.json.gz"
      // ordering so we stay readable across writer generations.
      return std::format("v{}.gz.metadata.json", version);
    case MetadataCompressionCodec::kZstd:
      // Kept for codec-name resolution symmetry; the encoder returns
      // NotSupported before any zstd file is actually emitted.
      return std::format("v{}.zstd.metadata.json", version);
  }
  return std::format("v{}.metadata.json", version);
}

std::string MetadataFilePath(std::string_view table_dir, int64_t version,
                             MetadataCompressionCodec codec) {
  return JoinUnderRoot(MetadataDir(table_dir), MetadataFileName(version, codec));
}

std::string VersionHintPath(std::string_view table_dir) {
  return JoinUnderRoot(MetadataDir(table_dir), "version-hint.text");
}

std::string LockFilePath(std::string_view table_dir) {
  return JoinUnderRoot(MetadataDir(table_dir), "_lock");
}

bool IsPathInside(std::string_view path, std::string_view parent_dir) {
  const std::string_view stripped_path = LocationUtil::StripTrailingSlash(path);
  const std::string_view stripped_parent = LocationUtil::StripTrailingSlash(parent_dir);
  if (stripped_path.size() < stripped_parent.size()) {
    return false;
  }
  if (!stripped_path.starts_with(stripped_parent)) {
    return false;
  }
  if (stripped_path.size() == stripped_parent.size()) {
    return true;  // exact match counts as "inside"
  }
  // Must be a true path boundary: the next character after parent_dir
  // has to be a separator. `/wh/db/stats_backup` shares the prefix
  // `/wh/db/stats` but the byte after the prefix is `_`, not `/`, so it
  // is a sibling, not a descendant.
  return stripped_path[stripped_parent.size()] == '/';
}

namespace {

// Percent-decode every `%HH` sequence in `s` into a fresh string. Returns
// false on malformed input (lone `%` or non-hex pair).
bool PercentDecode(std::string_view s, std::string& out) {
  auto hex_value = [](char c, int& v) {
    if (c >= '0' && c <= '9') {
      v = c - '0';
      return true;
    }
    if (c >= 'a' && c <= 'f') {
      v = c - 'a' + 10;
      return true;
    }
    if (c >= 'A' && c <= 'F') {
      v = c - 'A' + 10;
      return true;
    }
    return false;
  };
  out.clear();
  out.reserve(s.size());
  for (size_t i = 0; i < s.size();) {
    if (s[i] == '%') {
      if (i + 2 >= s.size()) {
        return false;
      }
      int hi = 0;
      int lo = 0;
      if (!hex_value(s[i + 1], hi) || !hex_value(s[i + 2], lo)) {
        return false;
      }
      out.push_back(static_cast<char>((hi << 4) | lo));
      i += 3;
    } else {
      out.push_back(s[i]);
      ++i;
    }
  }
  return true;
}

// True if any slash-separated segment of `path` equals "." or "..". The
// path is scanned in its already-decoded form; callers run PercentDecode
// first if they want to catch encoded traversal aliases. Both `/` and
// `\\` are treated as separators -- the latter would otherwise let a
// decoded `\..\` slip through on Windows (where backslash IS a
// directory separator) even though our POSIX-shaped layout never emits
// it. `\\..\\` between segments is also caught because we split on
// either separator.
bool ContainsTraversalSegment(std::string_view path) {
  size_t i = 0;
  while (i < path.size()) {
    const size_t next = path.find_first_of("/\\", i);
    const std::string_view seg =
        next == std::string_view::npos ? path.substr(i) : path.substr(i, next - i);
    if (seg == "." || seg == "..") {
      return true;
    }
    if (next == std::string_view::npos) {
      break;
    }
    i = next + 1;
  }
  return false;
}

}  // namespace

namespace {

// Detect whether `path` is a URI (has a `scheme://` prefix) or a bare
// filesystem path. ArrowFileSystemFileIO percent-decodes only the URI
// form; bare paths are passed to the OS literally. The descendant check
// must mirror that behaviour, otherwise a stats path like
// `/tmp/table%2Foutside/x.puffin` would be decoded by us into
// `/tmp/table/outside/x.puffin` and classified as "inside `/tmp/table`",
// while the literal access actually lands in the sibling directory
// `/tmp/table%2Foutside/`. Treat a leading `<scheme>://` as a URI; any
// other shape (including absolute POSIX paths and Windows drive paths)
// is literal.
bool LooksLikeUri(std::string_view path) {
  const auto colon = path.find(':');
  if (colon == std::string_view::npos || colon == 0) {
    return false;
  }
  // The substring before `:` must be a scheme: ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
  for (size_t i = 0; i < colon; ++i) {
    const char c = path[i];
    const bool ok =
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (i > 0 && ((c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.'));
    if (!ok) {
      return false;
    }
  }
  return path.size() > colon + 2 && path[colon + 1] == '/' && path[colon + 2] == '/';
}

}  // namespace

bool IsPathInsideNormalized(std::string_view path, std::string_view parent_dir) {
  // Only percent-decode if both inputs are URIs. ArrowFileSystemFileIO
  // only decodes the URI form; literal paths are passed through to the
  // OS unchanged, so decoding here would let `/tmp/t%2Foutside/x.puffin`
  // appear inside `/tmp/t` while the file actually lives in a sibling
  // directory `/tmp/t%2Foutside/`. If shapes differ between the two
  // inputs we conservatively refuse rather than guess; the caller is
  // expected to keep statistics/log paths in the same shape as the
  // table directory it stamped into the metadata.
  const bool path_is_uri = LooksLikeUri(path);
  const bool parent_is_uri = LooksLikeUri(parent_dir);
  if (path_is_uri != parent_is_uri) {
    return false;
  }
  if (!path_is_uri) {
    // Literal paths. Reject only obvious traversal aliases, never
    // percent-decode.
    if (ContainsTraversalSegment(path)) {
      return false;
    }
    return IsPathInside(path, parent_dir);
  }
  std::string decoded_path;
  if (!PercentDecode(path, decoded_path)) {
    return false;  // malformed percent encoding -- refuse
  }
  if (ContainsTraversalSegment(decoded_path)) {
    return false;  // `.` or `..` segment (after `/` or `\` split) post-decode
  }
  // parent_dir may itself contain legitimate percent encoding -- e.g. a
  // warehouse like `file:///tmp/my%20wh` would otherwise be compared
  // raw against the metadata's already-decoded paths and incorrectly
  // judged "outside". Decode both sides so the comparison is on
  // canonical form. Parent is built internally from a validated
  // identifier and the warehouse, so it carries no traversal segments
  // and we don't need to re-check them.
  std::string decoded_parent;
  if (!PercentDecode(parent_dir, decoded_parent)) {
    return false;
  }
  return IsPathInside(decoded_path, decoded_parent);
}

Status RejectAncestorIsTable(FileIO& file_io, std::string_view warehouse,
                             const Namespace& ns, bool last_inclusive) {
  const size_t stop = last_inclusive ? ns.levels.size() : ns.levels.size() - 1;
  if (ns.levels.empty()) {
    return {};
  }
  Namespace prefix;
  for (size_t i = 0; i < stop; ++i) {
    prefix.levels.push_back(ns.levels[i]);
    ICEBERG_ASSIGN_OR_RAISE(auto dir, NamespaceDir(warehouse, prefix));
    ICEBERG_ASSIGN_OR_RAISE(auto is_table, IsHadoopTableDir(file_io, dir));
    if (is_table) {
      return InvalidArgument(
          "Refusing operation under '{}': an ancestor at '{}' is already a "
          "Hadoop table; creating a namespace or table beneath it would be "
          "wiped by a subsequent DropTable(purge=true) of that ancestor.",
          ns.ToString(), dir);
    }
  }
  return {};
}

Result<bool> HasNonTableInternalChildren(FileIO& file_io, std::string_view dir_location) {
  const std::string dir{dir_location};
  ICEBERG_ASSIGN_OR_RAISE(auto exists, file_io.Exists(dir));
  if (!exists) {
    return false;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto is_dir, file_io.IsDirectory(dir));
  if (!is_dir) {
    return false;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto entries, file_io.ListDir(dir));
  for (const auto& entry : entries) {
    const std::string_view leaf = Basename(entry.location);
    if (leaf.empty() || leaf == "metadata" || leaf == "data") {
      continue;
    }
    return true;
  }
  return false;
}

Result<bool> IsHadoopTableDir(FileIO& file_io, std::string_view dir_location) {
  // A HadoopCatalog table contains a metadata/ subdirectory with at least one
  // v{N}.metadata.json[.codec] file. We probe the metadata dir first and only
  // list its contents if it exists, to avoid expensive listings on plain
  // namespace directories.
  const std::string metadata_dir = MetadataDir(dir_location);
  ICEBERG_ASSIGN_OR_RAISE(auto exists, file_io.Exists(metadata_dir));
  if (!exists) {
    return false;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto is_dir, file_io.IsDirectory(metadata_dir));
  if (!is_dir) {
    return false;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto entries, file_io.ListDir(metadata_dir));
  for (const auto& entry : entries) {
    if (entry.is_directory) {
      continue;
    }
    if (ParseMetadataFileName(Basename(entry.location)).has_value()) {
      return true;
    }
  }
  return false;
}

Result<MetadataFileRef> ParseMetadataFileName(std::string_view file_name) {
  if (!file_name.starts_with(kVersionPrefix)) {
    return InvalidArgument(
        "Metadata filename '{}' does not start with 'v' (expected vN.metadata.json).",
        file_name);
  }

  // Accept the two gzip filename shapes the core library defines:
  //   - canonical: v{N}.gz.metadata.json (TableMetadataUtil::Codec current form)
  //   - legacy:    v{N}.metadata.json.gz (backwards-compat)
  // The legacy form is still emitted by older writers (including some
  // historic Hadoop catalog releases) and must remain discoverable.
  //
  // zstd is deliberately NOT recognised here. The encoder side returns
  // NotSupported for zstd, so a v{N}.metadata.json.zstd file in the
  // metadata dir is from an unknown external writer that iceberg-cpp's
  // reader cannot decode. Accepting it on the listing side would make
  // TableExists return true while LoadTable failed mid-JSON-parse, which
  // is worse than reporting "no such table" cleanly.
  std::string_view stripped = file_name;
  MetadataCompressionCodec codec = MetadataCompressionCodec::kNone;
  // Handle the legacy gzip shape `v{N}.metadata.json.gz` by trimming the
  // trailing `.gz` first; the canonical shape `v{N}.gz.metadata.json` is
  // handled below by stripping `.gz` from the version segment after the
  // `.metadata.json` suffix is removed.
  if (stripped.ends_with(".metadata.json.gz")) {
    codec = MetadataCompressionCodec::kGzip;
    stripped.remove_suffix(std::string_view(".gz").size());
  }
  if (!stripped.ends_with(kJsonSuffix)) {
    return InvalidArgument("Metadata filename '{}' does not end with '.metadata.json'.",
                           file_name);
  }
  stripped.remove_suffix(kJsonSuffix.size());

  // Canonical gzip form puts the `.gz` between the version and
  // `.metadata.json`; pick it up if we haven't already locked codec from
  // the legacy shape.
  if (codec == MetadataCompressionCodec::kNone && stripped.ends_with(".gz")) {
    codec = MetadataCompressionCodec::kGzip;
    stripped.remove_suffix(std::string_view(".gz").size());
  }

  // Remaining piece must be `v<digits>`.
  std::string_view version_str = stripped;
  version_str.remove_prefix(kVersionPrefix.size());
  if (version_str.empty()) {
    return InvalidArgument("Metadata filename '{}' has empty version segment.",
                           file_name);
  }
  auto version = StringUtils::ParseNumber<int64_t>(version_str);
  if (!version.has_value()) {
    return InvalidArgument("Metadata filename '{}' has non-numeric version segment '{}'.",
                           file_name, version_str);
  }
  return MetadataFileRef{.version = *version, .codec = codec};
}

}  // namespace iceberg::hadoop
