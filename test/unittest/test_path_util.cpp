#include "catch/catch.hpp"

#include "path_util.hpp"

using namespace duckdb;

TEST_CASE("ExtractBucket - S3 paths", "[path_util]") {
	REQUIRE(ExtractBucket("s3://my-bucket/path/to/file.csv") == "my-bucket");
	REQUIRE(ExtractBucket("s3://another-bucket/data.parquet") == "another-bucket");
	REQUIRE(ExtractBucket("s3://bucket-with-hyphens/file.txt") == "bucket-with-hyphens");
	REQUIRE(ExtractBucket("s3://bucket_with_underscores/file.txt") == "bucket_with_underscores");
	REQUIRE(ExtractBucket("s3://bucket123/file.txt") == "bucket123");
}

TEST_CASE("ExtractBucket - GCS paths", "[path_util]") {
	REQUIRE(ExtractBucket("gcs://gcs-bucket/object.json") == "gcs-bucket");
	REQUIRE(ExtractBucket("gcs://my-gcs-bucket/folder/file.parquet") == "my-gcs-bucket");
}

TEST_CASE("ExtractBucket - Azure paths", "[path_util]") {
	REQUIRE(ExtractBucket("az://azure-bucket/data.csv") == "azure-bucket");
	REQUIRE(ExtractBucket("abfss://container@account.dfs.core.windows.net/path") ==
	        "container@account.dfs.core.windows.net");
}

TEST_CASE("ExtractBucket - Local paths return empty", "[path_util]") {
	REQUIRE(ExtractBucket("/local/path/file.txt") == "");
	REQUIRE(ExtractBucket("/tmp/test.csv") == "");
	REQUIRE(ExtractBucket("./relative/path.txt") == "");
	REQUIRE(ExtractBucket("relative/path.txt") == "");
	REQUIRE(ExtractBucket("file.txt") == "");
}

TEST_CASE("ExtractBucket - Windows paths return empty", "[path_util]") {
	REQUIRE(ExtractBucket("C:\\Users\\test\\file.txt") == "");
	REQUIRE(ExtractBucket("D:\\data\\file.csv") == "");
}

TEST_CASE("ExtractBucket - HTTP/HTTPS URLs", "[path_util]") {
	REQUIRE(ExtractBucket("http://example.com/file.txt") == "example.com");
	REQUIRE(ExtractBucket("https://api.example.com/data.json") == "api.example.com");
}

TEST_CASE("ExtractBucket - Edge cases", "[path_util]") {
	// Empty path
	REQUIRE(ExtractBucket("") == "");

	// Scheme only
	REQUIRE(ExtractBucket("s3://") == "");

	// Bucket with no path
	REQUIRE(ExtractBucket("s3://bucket") == "bucket");

	// Bucket with trailing slash
	REQUIRE(ExtractBucket("s3://bucket/") == "bucket");

	// Complex nested paths
	REQUIRE(ExtractBucket("s3://bucket/dir1/dir2/dir3/file.txt") == "bucket");
}

TEST_CASE("ExtractBucket - Special characters in bucket names", "[path_util]") {
	REQUIRE(ExtractBucket("s3://bucket.with.dots/file.txt") == "bucket.with.dots");
	REQUIRE(ExtractBucket("s3://bucket-with-many-hyphens/file.txt") == "bucket-with-many-hyphens");
}

TEST_CASE("ExtractBucket - File scheme paths", "[path_util]") {
	REQUIRE(ExtractBucket("file:///local/path/file.txt") == "");
	REQUIRE(ExtractBucket("file://localhost/path/file.txt") == "localhost");
}
