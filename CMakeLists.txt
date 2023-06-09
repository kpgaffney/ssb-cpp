cmake_minimum_required(VERSION 3.14)
project(ssb_cpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

find_package(SQLite3 REQUIRED)

include(FetchContent)

FetchContent_Declare(
        tbb
        GIT_REPOSITORY https://github.com/oneapi-src/oneTBB.git
        GIT_TAG v2021.6.0
)
set(TBB_TEST OFF)
FetchContent_MakeAvailable(tbb)

FetchContent_Declare(
        abseil
        GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
        GIT_TAG 20230125.2
)
set(ABSL_PROPAGATE_CXX_STD ON)
FetchContent_MakeAvailable(abseil)

add_executable(
        ssb_cpp
        src/common.hpp
        src/queries/q1.cpp
        src/queries/q2.cpp
        src/queries/q3.cpp
        src/queries/q4.cpp
        src/main.cpp
)
target_link_libraries(ssb_cpp SQLite::SQLite3 TBB::tbb absl::base absl::flat_hash_set absl::flat_hash_map)
