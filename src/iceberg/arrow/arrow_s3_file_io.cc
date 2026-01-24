#include "iceberg/arrow/arrow_s3_file_io.h"

#if defined(HAVE_ARROW_S3)
#include <arrow/filesystem/s3fs.h>
#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/arrow/arrow_status_internal.h"

namespace iceberg::arrow {

std::unique_ptr<FileIO> MakeS3FileIO(const ::arrow::fs::S3Options& options) {
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto s3fs, ::arrow::fs::S3FileSystem::Make(options));
  return std::make_unique<ArrowFileSystemFileIO>(s3fs);
}

} // namespace iceberg::arrow

#endif
