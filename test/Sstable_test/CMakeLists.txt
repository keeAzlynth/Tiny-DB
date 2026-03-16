cmake_minimum_required(VERSION 4.0)
project(LSM_Sstable_Test)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -ggdb3 -fdebug-prefix-map=/=/")
# 选项配置
option(ENABLE_TSAN "启用 ThreadSanitizer" OFF)
option(ENABLE_VALGRIND "启用 Valgrind 支持" OFF)
option(ENABLE_ASAN "启用 AddressSanitizer" OFF)

# TSAN 配置
if(ENABLE_TSAN)
    add_compile_options(-fsanitize=thread -g)
    add_link_options(-fsanitize=thread)
endif()

# ASAN 配置
if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

# Valgrind 配置
if(ENABLE_VALGRIND)
    add_compile_options(-g -O0)
endif()

# 检查冲突
if(ENABLE_TSAN AND ENABLE_VALGRIND)
    message(FATAL_ERROR "ThreadSanitizer 和 Valgrind 不能同时启用")
endif()

if(ENABLE_ASAN AND ENABLE_VALGRIND)
    message(FATAL_ERROR "AddressSanitizer 和 Valgrind 不能同时启用")
endif()

if(ENABLE_TSAN AND ENABLE_ASAN)
    message(FATAL_ERROR "ThreadSanitizer 和 AddressSanitizer 不能同时启用")
endif()

# Try include top-level generated options so this subproject respects top-level switches.
if(EXISTS ${CMAKE_SOURCE_DIR}/../LSMTestOptions.cmake)
    include(${CMAKE_SOURCE_DIR}/../LSMTestOptions.cmake)
endif()

# If the option is not defined (user configures this test directory directly),
# default to ON so local per-test builds remain convenient.
if(NOT DEFINED ENABLE_SSTABLE_TEST)
    set(ENABLE_SSTABLE_TEST ON)
endif()

if(NOT ENABLE_SSTABLE_TEST)
    message(STATUS "Sstable_test disabled (ENABLE_SSTABLE_TEST=OFF)")
    return()
endif()

# 查找 GTest 包
find_package(GTest REQUIRED)

# 源文件列表
set(SOURCE_FILES
    ../../src/Sstable.cpp
    ../../src/SstableIterator.cpp
    ../../src/Block.cpp
    ../../src/BlockIterator.cpp
    ../../src/Blockcache.cpp
    ../../src/std_file.cpp
    ../../src/mmap.cpp
    ../../src/file.cpp
    ../../src/BloomFilter.cpp
    ../../src/BlockMeta.cpp
    ../../src/memtable.cpp
    ../../src/Skiplist.cpp
    ../../src/Baselterator.cpp
    ../../src/Global.cpp
)

# 创建测试可执行文件
add_executable(sstable_test
    sstable_test.cpp
    ${SOURCE_FILES}
)

# 添加头文件路径
target_include_directories(sstable_test PRIVATE ../../include)

# 链接 Google Test 和线程库（使用现代CMake目标）
target_link_libraries(sstable_test PRIVATE 
    GTest::gtest 
    GTest::gtest_main 
    pthread
)

# 启用测试
enable_testing()
add_test(NAME sstable_test COMMAND sstable_test)

# 添加 Valgrind 内存检查目标
if(ENABLE_VALGRIND)
    find_program(VALGRIND_EXECUTABLE valgrind)
    if(VALGRIND_EXECUTABLE)
        add_custom_target(valgrind-check
            COMMAND ${VALGRIND_EXECUTABLE}
                --leak-check=full
                --show-leak-kinds=all
                --error-exitcode=1
                ./sstable_test
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
            COMMENT "运行 Valgrind 内存检查"
        )
    endif()
endif()