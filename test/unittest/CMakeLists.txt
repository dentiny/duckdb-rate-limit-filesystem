include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

set(RATE_LIMITER_UNITTEST_OBJECTS
    test_burst_limit.cpp
    test_counting_semaphore.cpp
    test_filesystem_glob.cpp
    test_max_requests.cpp
    test_no_destructor.cpp
    test_rate_limit.cpp
    test_rate_limit_file_system.cpp
    test_rate_limit_file_system_mock.cpp
    test_scoped_directory.cpp)

add_executable(unittest_rate_limiter main.cpp ${RATE_LIMITER_UNITTEST_OBJECTS})

if(NOT WIN32
   AND NOT SUN
   AND NOT ZOS)
  target_link_libraries(unittest_rate_limiter test_helpers duckdb
                        ${EXTENSION_NAME})
else()
  target_link_libraries(unittest_rate_limiter test_helpers duckdb_static
                        ${EXTENSION_NAME})
endif()
