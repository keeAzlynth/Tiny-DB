cmake_minimum_required(VERSION 3.20)

project(LSM_Tests NONE)

# This CMake file is intended to be simple: it adds test subdirectories
# only when the corresponding top-level option is enabled.

if(ENABLE_BLOCK_TEST)
  add_subdirectory(Block_test)
endif()

if(ENABLE_BLOCKMETA_TEST)
  add_subdirectory(BlockMeta_test)
endif()

if(ENABLE_MEMTABLE_TEST)
  add_subdirectory(Memtable_test)
endif()

if(ENABLE_SKIPLIST_TEST)
  add_subdirectory(Skiplist_test)
endif()

if(ENABLE_SSTABLE_TEST)
  add_subdirectory(Sstable_test)
endif()

if(ENABLE_LSM_TEST)
  add_subdirectory(LSM_test)
endif()
