# --- iceberg project CMake additions for Arrow S3 support ---
option(ICEBERG_ENABLE_ARROW_S3 "Enable Arrow S3 integration (requires Arrow built with S3 support)" ON)

if(ICEBERG_ENABLE_ARROW_S3)
  # Define a compile-time macro so source can conditionally include Arrow S3 headers.
  add_compile_definitions(HAVE_ARROW_S3)
endif()

# Note: For robustness, consider testing whether the installed Arrow
# library actually provides s3fs.h rather than relying solely on this option.
