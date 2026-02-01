include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

file(GLOB_RECURSE RATE_LIMITER_UNITTEST_OBJECTS
     "${CMAKE_CURRENT_SOURCE_DIR}/test_*.cpp")

add_executable(unittest_rate_limiter main.cpp ${RATE_LIMITER_UNITTEST_OBJECTS})

if(NOT WIN32
   AND NOT SUN
   AND NOT ZOS)
  target_link_libraries(unittest_rate_limiter test_helpers duckdb)
else()
  target_link_libraries(unittest_rate_limiter test_helpers duckdb_static)
endif()
