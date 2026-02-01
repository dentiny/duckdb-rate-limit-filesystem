include_directories(include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../src)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../src/include)
include_directories(${DuckDB_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

file(GLOB_RECURSE CATCHFS_UNITTEST_OBJECTS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

set(THROTTLE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/../../src/throttle_layer.cpp
)

add_executable(unittest_rate_limiter main.cpp ${CATCHFS_UNITTEST_OBJECTS} ${THROTTLE_SOURCES})

set_target_properties(unittest_rate_limiter PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON)

if(NOT WIN32
   AND NOT SUN
   AND NOT ZOS)
  target_link_libraries(unittest_rate_limiter test_helpers duckdb)
else()
  target_link_libraries(unittest_rate_limiter test_helpers duckdb_static)
endif()
