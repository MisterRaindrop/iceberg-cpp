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

#include "iceberg/result.h"

namespace iceberg {

class ZlibImpl;

class GZipDecompressor {
 public:
  GZipDecompressor();

  ~GZipDecompressor();

  Status Init();

  Result<std::string> Decompress(const std::string& compressed_data);

 private:
  std::unique_ptr<ZlibImpl> zlib_impl_;
};

class GZipCompressor {
 public:
  GZipCompressor();

  ~GZipCompressor();

  /// \brief Initialise the deflate stream. Safe to call lazily; Compress()
  /// will invoke it on first use if the caller does not.
  Status Init();

  /// \brief Compress \p data using gzip framing (zlib's windowBits = 15 + 16).
  /// Returns the compressed bytes; the empty input case round-trips to an
  /// empty (or near-empty) gzip frame -- callers should not rely on the
  /// exact representation of empty input.
  Result<std::string> Compress(const std::string& data);

 private:
  class ZlibDeflateImpl;
  std::unique_ptr<ZlibDeflateImpl> zlib_impl_;
};

}  // namespace iceberg
