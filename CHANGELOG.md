# 0.1.0

## Changed

- Upgrade DuckDB to `v2.0.0` and extension-ci-tools to the latest commit.
- Use C++17 and DuckDB identifiers for table function columns.
- Replace copied thread annotation and lock implementations with DuckDB utilities.
- Use explicit vector append/cardinality APIs and C++17 string views and type traits.

## Fixed

- Protect configuration snapshot sizing with its configuration mutex.
- Link unit tests correctly against prebuilt DuckDB static libraries.

# 0.0.10

## Changed

- Update DuckDB and extension-ci-tools to `v1.5.6`.

## Added

- Add descriptions, examples, categories, and parameter names for all exposed
  extension functions in `duckdb_functions()`.

# 0.0.9

## Changed

- Update DuckDB core and extension-ci-tools to v1.5.5

# 0.0.8

## Changed

- Update DuckDB core and extension-ci-tools to v1.5.4

# 0.0.7

## Changed

- Update DuckDB core and extension-ci-tools to v1.5.3

# 0.0.6

## Fixed

- Add missing filesystem API ([#33])

[#33]: https://github.com/dentiny/duckdb-rate-limit-filesystem/pull/33

## Added

- Add rate limit for directory creation ([#35])

[#35]: https://github.com/dentiny/duckdb-rate-limit-filesystem/pull/35

# 0.0.5

## Improved

- Improve concurrent access ([#31])

[#31]: https://github.com/dentiny/duckdb-rate-limit-filesystem/pull/31

## Changed

- Update DuckDB core and extension-ci-tools to v1.5.1 ([#30])

[#30]: https://github.com/dentiny/duckdb-rate-limit-filesystem/pull/30

## Fixed

- Fix glob implementation if extended version supported ([#32])

[#32]: https://github.com/dentiny/duckdb-rate-limit-filesystem/pull/32

# 0.0.4

## Add

- Add operation-level concurrent limit ([#29])

[#29]: https://github.com/dentiny/duckdb-rate-limit-filesystem/pull/29

# 0.0.3

## Changed

- Update extension-ci-tools to latest, which points to duckdb v1.5.0 

## Added

- Add an extension description

# 0.0.2

## Changed

- Update extension-ci-tools to latest, which points to duckdb v1.4.4
