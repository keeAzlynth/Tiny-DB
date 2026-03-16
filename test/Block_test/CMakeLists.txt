cmake_minimum_required(VERSION 4.0)
project(Block_Test)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 选项配置
option(ENABLE_ASAN "启用 AddressSanitizer" OFF)
option(ENABLE_VALGRIND "启用 Valgrind 支持" OFF)

# ASAN 配置
if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address)
endif()

# Valgrind 配置
if(ENABLE_VALGRIND)
    add_compile_options(-g -O0)
endif()

# 检查冲突
if(ENABLE_ASAN AND ENABLE_VALGRIND)
    message(FATAL_ERROR "ASAN 和 Valgrind 不能同时启用")
endif()

# Try include top-level generated options so this subproject respects top-level switches.
if(EXISTS ${CMAKE_SOURCE_DIR}/../LSMTestOptions.cmake)
    include(${CMAKE_SOURCE_DIR}/../LSMTestOptions.cmake)
endif()

if(NOT DEFINED ENABLE_BLOCK_TEST)
    set(ENABLE_BLOCK_TEST ON)
endif()

if(NOT ENABLE_BLOCK_TEST)
    message(STATUS "Block_test disabled (ENABLE_BLOCK_TEST=OFF)")
    return()
endif()

# 查找 GTest 包
find_package(GTest REQUIRED)

# 添加源文件
set(SOURCE_FILES
    ../../src/Block.cpp
    ../../src/BlockIterator.cpp
)

# 创建测试可执行文件
add_executable(block_test
    Blocktest.cpp
    ${SOURCE_FILES}
)

# 链接库
target_link_libraries(block_test
    GTest::gtest
    GTest::gtest_main
    pthread
)

# 添加头文件路径
target_include_directories(block_test PRIVATE
    ../../include
)

# 启用测试
enable_testing()
add_test(NAME BlockTest COMMAND block_test)

# 添加 Valgrind 内存检查目标
if(ENABLE_VALGRIND)
    find_program(VALGRIND_EXECUTABLE valgrind)
    if(VALGRIND_EXECUTABLE)
        add_custom_target(valgrind-check
            COMMAND ${VALGRIND_EXECUTABLE}
                --leak-check=full
                --show-leak-kinds=all
                --track-origins=yes
                --error-exitcode=1
                ./block_test
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
            COMMENT "运行 Valgrind 内存检查"
            VERBATIM
        )
    else()
        message(WARNING "Valgrind 未找到，跳过 valgrind-check 目标")
    endif()
endif()

# 添加 ASAN 运行说明
if(ENABLE_ASAN)
    add_custom_target(run-asan-test
        COMMAND ./block_test
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "运行带 ASAN 的测试"
        VERBATIM
    )
endif()