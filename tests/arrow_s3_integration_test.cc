// Integration test for Arrow S3-backed FileIO.
// This test is optional and only runs when the environment variable
// ICEBERG_S3_INTEGRATION is set to "1" and Arrow is built with S3 support.

#include <gtest/gtest.h>
#include <cstdlib>
#include <string>

#if defined(HAVE_ARROW_S3)
#include <arrow/filesystem/s3fs.h>
#include "iceberg/arrow/arrow_s3_file_io.h"
#endif

TEST(ArrowS3Integration, CreateReadDeleteObject) {
  const char* run = std::getenv("ICEBERG_S3_INTEGRATION");
  if (!run || std::string(run) != "1") {
    GTEST_SKIP() << "ICEBERG_S3_INTEGRATION not set; skipping S3 integration test";
  }

#if !defined(HAVE_ARROW_S3)
  GTEST_SKIP() << "Arrow not built with S3 support; skipping S3 integration test";
#else
  // Read S3 connection info from env. Tests should set these for MinIO/AWS.
  const char* endpoint = std::getenv("ICEBERG_S3_ENDPOINT");
  const char* access_key = std::getenv("ICEBERG_S3_ACCESS_KEY");
  const char* secret_key = std::getenv("ICEBERG_S3_SECRET_KEY");
  const char* bucket = std::getenv("ICEBERG_S3_BUCKET");

  ASSERT_NE(endpoint, nullptr);
  ASSERT_NE(access_key, nullptr);
  ASSERT_NE(secret_key, nullptr);
  ASSERT_NE(bucket, nullptr);

  ::arrow::fs::S3Options s3_opts = ::arrow::fs::S3Options::Anonymous();
  // Fill required options. Adjust fields per Arrow version's S3Options API.
  s3_opts.endpoint_override = std::string(endpoint);
  s3_opts.region = "us-east-1";
  s3_opts.use_virtual_host_style = false;
  s3_opts.access_key = std::string(access_key);
  s3_opts.secret_key = std::string(secret_key);

  auto fileio = iceberg::arrow::MakeS3FileIO(s3_opts);
  ASSERT_NE(fileio, nullptr);

  std::string key = std::string("iceberg_test/arrow_s3_integration_test.txt");
  std::string uri = std::string("s3://") + bucket + "/" + key;

  const std::string payload = "hello iceberg s3 integration";
  ASSERT_TRUE(fileio->WriteFile(uri, payload).ok());

  auto result = fileio->ReadFile(uri);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->size(), payload.size());
  EXPECT_EQ(*result, payload);

  ASSERT_TRUE(fileio->DeleteFile(uri).ok());
#endif
}
