#pragma once

#include <memory>

#if defined(HAVE_ARROW_S3)
#include <arrow/filesystem/s3fs.h>
#endif

#include "iceberg/file_io.h"

namespace iceberg::arrow {

#if defined(HAVE_ARROW_S3)
// Create a FileIO backed by Arrow's S3FileSystem using the provided options.
std::unique_ptr<FileIO> MakeS3FileIO(const ::arrow::fs::S3Options& options);
#endif

} // namespace iceberg::arrow
