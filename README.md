# Rate Limit Filesystem Extension for DuckDB

A DuckDB extension that provides rate limiting capabilities for filesystem operations. This extension allows you to control the bandwidth and rate of file system operations (read, write, list, stat, delete) to prevent overwhelming storage systems or comply with API rate limits.

## Loading rate limit filesystem
Since DuckDB v1.0.0, cache httpfs can be loaded as a community extension without requiring the `unsigned` flag. From any DuckDB instance, the following two commands will allow you to install and load the extension:
```sql
INSTALL rate_limit_fs from community;
-- Or upgrade to latest version with `FORCE INSTALL rate_limit_fs from community;`
LOAD rate_limit_fs;
```

## What Does This Project Do?

The Rate Limit Filesystem extension wraps any DuckDB filesystem with rate limiting functionality. It provides:

- **Bandwidth Control**: Limit the rate of file operations (bytes per second for read/write, operations per second for list/stat/delete)
- **Burst Support**: Allow temporary bursts of activity while maintaining long-term rate limits
- **Multiple Operation Types**: Rate limit different operation types independently (read, write, list, stat, delete)
- **Two Modes**: 
  - **Blocking mode**: Operations wait until the rate limit allows them to proceed
  - **Non-blocking mode**: Operations fail immediately if the rate limit would be exceeded
- **Per-Filesystem Configuration**: Apply different rate limits to different filesystems

The extension uses the [GCRA (Generic Cell Rate Algorithm)](https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm) rate limiting algorithm, instead of [token bucket algorithm](https://en.wikipedia.org/wiki/Token_bucket) so tokens don't need to be updated periodically, which provides smooth rate limiting with burst support and nanosecond-precision timing.

## How to Use

### Step 1: Wrap a Filesystem

First, you need to wrap an existing filesystem with rate limiting. List available filesystems:

```sql
SELECT * FROM rate_limit_fs_list_filesystems();
```

Then wrap the filesystem you want to rate limit:

```sql
SELECT rate_limit_fs_wrap('LocalFileSystem');
```

After wrapping, the filesystem will be renamed to `RateLimitFileSystem - <original_name>`. For example, `LocalFileSystem` becomes `RateLimitFileSystem - LocalFileSystem`.

### Step 2: Configure Rate Limits

Set rate limits for specific operations. You can configure:
- **Quota**: The rate limit (bytes/sec for read/write, operations/sec for others)
- **Mode**: `blocking` (wait) or `non_blocking` (fail immediately)
- **Burst**: Maximum bytes allowed in a single burst (only for read/write operations)

#### Example: Limit Read Operations

```sql
-- Set read rate limit: 1 MB/sec, blocking mode
SELECT rate_limit_fs_quota('RateLimitFileSystem - LocalFileSystem', 'read', 1048576, 'blocking');

-- Set burst limit: allow up to 10 MB in a single read
SELECT rate_limit_fs_burst('RateLimitFileSystem - LocalFileLimitFileSystem', 'read', 10485760);
```

#### Example: Limit Write Operations

```sql
-- Set write rate limit: 500 KB/sec, non-blocking mode
SELECT rate_limit_fs_quota('RateLimitFileSystem - LocalFileSystem', 'write', 512000, 'non_blocking');

-- Set burst limit: allow up to 5 MB in a single write
SELECT rate_limit_fs_burst('RateLimitFileSystem - LocalFileSystem', 'write', 5242880);
```

#### Example: Limit List Operations

```sql
-- Allow only 10 list operations per second, non-blocking
SELECT rate_limit_fs_quota('RateLimitFileSystem - LocalFileSystem', 'list', 10, 'non_blocking');
```

### Step 3: Use the Rate-Limited Filesystem

Once configured, all operations on the wrapped filesystem will be rate limited:

```sql
-- This read will be rate limited
SELECT * FROM read_csv('/path/to/file.csv');

-- This write will be rate limited
COPY (SELECT * FROM my_table) TO '/path/to/output.csv';

-- This list operation will be rate limited
SELECT * FROM glob('/path/to/directory/*');
```

### Step 4: View and Manage Configurations

View all current rate limit configurations:

```sql
SELECT * FROM rate_limit_fs_configs();
```

Clear a specific configuration:

```sql
-- Clear read rate limit for a filesystem
SELECT rate_limit_fs_clear('RateLimitFileSystem - LocalFileSystem', 'read');

-- Clear all rate limits for a filesystem
SELECT rate_limit_fs_clear('RateLimitFileSystem - LocalFileSystem', '*');

-- Clear all rate limits for all filesystems
SELECT rate_limit_fs_clear('*', '*');
```

## Key Functions

### Scalar Functions

#### `rate_limit_fs_wrap(filesystem_name)`
Wraps an existing filesystem with rate limiting capabilities.

- **Parameters**: 
  - `filesystem_name` (VARCHAR): Name of the filesystem to wrap
- **Returns**: BOOLEAN (true on success)
- **Example**: `SELECT rate_limit_fs_wrap('LocalFileSystem');`

#### `rate_limit_fs_quota(filesystem_name, operation, value, mode)`
Sets the rate limit quota for a specific operation on a filesystem.

- **Parameters**:
  - `filesystem_name` (VARCHAR): Name of the wrapped filesystem (use the name returned after wrapping)
  - `operation` (VARCHAR): Operation type: `'read'`, `'write'`, `'list'`, `'stat'`, or `'delete'`
  - `value` (BIGINT): Rate limit value (bytes/sec for read/write, operations/sec for others)
  - `mode` (VARCHAR): `'blocking'` or `'non_blocking'`
- **Returns**: BOOLEAN (true on success)
- **Example**: `SELECT rate_limit_fs_quota('RateLimitFileSystem - LocalFileSystem', 'read', 1048576, 'blocking');`

#### `rate_limit_fs_burst(filesystem_name, operation, value)`
Sets the burst limit for read or write operations.

- **Parameters**:
  - `filesystem_name` (VARCHAR): Name of the wrapped filesystem
  - `operation` (VARCHAR): `'read'` or `'write'` (only these operations support burst)
  - `value` (BIGINT): Maximum bytes allowed in a single burst
- **Returns**: BOOLEAN (true on success)
- **Example**: `SELECT rate_limit_fs_burst('RateLimitFileSystem - LocalFileSystem', 'read', 10485760);`

#### `rate_limit_fs_clear(filesystem_name, operation)`
Clears rate limit configuration(s).

- **Parameters**:
  - `filesystem_name` (VARCHAR): Filesystem name, or `'*'` to clear all filesystems
  - `operation` (VARCHAR): Operation name, or `'*'` to clear all operations for the filesystem
- **Returns**: BOOLEAN (true on success)
- **Example**: `SELECT rate_limit_fs_clear('RateLimitFileSystem - LocalFileSystem', 'read');`

### Table Functions

#### `rate_limit_fs_list_filesystems()`
Lists all available filesystems in the DuckDB virtual filesystem.

- **Returns**: Table with columns:
  - `name` (VARCHAR): Filesystem name
- **Example**: `SELECT * FROM rate_limit_fs_list_filesystems();`

#### `rate_limit_fs_configs()`
Lists all current rate limit configurations.

- **Returns**: Table with columns:
  - `filesystem` (VARCHAR): Filesystem name
  - `operation` (VARCHAR): Operation type
  - `quota` (BIGINT): Rate limit quota
  - `mode` (VARCHAR): Rate limit mode
  - `burst` (BIGINT): Burst limit (0 if not set)
- **Example**: `SELECT * FROM rate_limit_fs_configs();`

## Supported Operations

The extension can rate limit the following filesystem operations:

- **READ**: Reading data from files (bytes/sec)
- **WRITE**: Writing data to files (bytes/sec)
- **LIST**: Listing directory contents via `glob()` or `list_files()` (operations/sec)
- **STAT**: File metadata operations like `file_exists()`, `get_file_size()` (operations/sec)
- **DELETE**: Deleting files or directories (operations/sec)

## Rate Limiting Modes

### Blocking Mode
When a rate limit would be exceeded, the operation waits until the rate limit allows it to proceed. This ensures operations complete successfully but may take longer.

```sql
SELECT rate_limit_fs_quota('RateLimitFileSystem - LocalFileSystem', 'read', 1048576, 'blocking');
```

### Non-Blocking Mode
When a rate limit would be exceeded, the operation fails immediately with an error. This is useful when you want to fail fast rather than wait.

```sql
SELECT rate_limit_fs_quota('RateLimitFileSystem - LocalFileSystem', 'read', 1048576, 'non_blocking');
```

## Complete Example

```sql
-- Load the extension
FORCE INSTALL rate_limit_fs FROM community;
LOAD rate_limit_fs;

-- List available filesystems
SELECT * FROM rate_limit_fs_list_filesystems();

-- Wrap the local filesystem
SELECT rate_limit_fs_wrap('LocalFileSystem');

-- Configure read rate limit: 1 MB/sec with 10 MB burst, blocking
SELECT rate_limit_fs_quota('RateLimitFileSystem - LocalFileSystem', 'read', 1048576, 'blocking');
SELECT rate_limit_fs_burst('RateLimitFileSystem - LocalFileSystem', 'read', 10485760);

-- Configure write rate limit: 500 KB/sec with 5 MB burst, non-blocking
SELECT rate_limit_fs_quota('RateLimitFileSystem - LocalFileSystem', 'write', 512000, 'non_blocking');
SELECT rate_limit_fs_burst('RateLimitFileSystem - LocalFileSystem', 'write', 5242880);

-- View all configurations
SELECT * FROM rate_limit_fs_configs();

-- Use the rate-limited filesystem (operations will be rate limited automatically, no need to change existing code)
SELECT * FROM read_csv('/path/to/large_file.csv');
COPY (SELECT * FROM my_table) TO '/path/to/output.csv';

-- Clean up: clear all configurations on all filesystems
SELECT rate_limit_fs_clear('*', '*');
```
