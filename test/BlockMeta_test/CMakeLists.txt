cmake_minimum_required(VERSION 4.0)
project(BlockMetaTest)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address  -g")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")

# Try include top-level generated options so this subproject respects top-level switches.
if(EXISTS ${CMAKE_SOURCE_DIR}/../LSMTestOptions.cmake)
    include(${CMAKE_SOURCE_DIR}/../LSMTestOptions.cmake)
endif()

if(NOT DEFINED ENABLE_BLOCKMETA_TEST)
    set(ENABLE_BLOCKMETA_TEST ON)
endif()

if(NOT ENABLE_BLOCKMETA_TEST)
    message(STATUS "BlockMeta_test disabled (ENABLE_BLOCKMETA_TEST=OFF)")
    return()
endif()

# 查找 GTest 包
find_package(GTest REQUIRED)
find_package(spdlog REQUIRED)
find_package(fmt QUIET)

if(NOT fmt_FOUND)
    target_compile_definitions(spdlog::spdlog INTERFACE SPDLOG_FMT_EXTERNAL=OFF)
endif()

include_directories(${GTEST_INCLUDE_DIRS})

# 添加源文件
add_executable(blockmeta_test
    BlockMeta_test.cpp
    ../../src/storage/Sstable.cpp
    ../../src/iterator/SstableIterator.cpp
    ../../src/storage/Block.cpp
    ../../src/iterator/BlockIterator.cpp
    ../../src/storage/Blockcache.cpp
    ../../src/storage/std_file.cpp
    ../../src/storage/mmap.cpp
    ../../src/storage/file.cpp
    ../../src/storage/BloomFilter.cpp
    ../../src/storage/BlockMeta.cpp
    ../../src/core/memtable.cpp
    ../../src/core/Skiplist.cpp
    ../../src/iterator/Baselterator.cpp
    ../../src/core/Global.cpp
)

# 链接 Google Test 库
target_link_libraries(blockmeta_test
    PRIVATE
    GTest::GTest
    GTest::Main
    spdlog::spdlog
    pthread
)

if(TARGET fmt::fmt)
    target_link_libraries(blockmeta_test PRIVATE fmt::fmt)
endif()

# 添加头文件路径
target_include_directories(blockmeta_test PRIVATE
    ../../include
)

# 启用测试
enable_testing()
add_test(NAME blockmeta_test COMMAND blockmeta_test)
