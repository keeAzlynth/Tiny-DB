#pragma once
#include <random>
namespace Global_ {
constexpr int    FIX_LEVEL                         = 4;
constexpr int    MAX_LEVEL                         = 12;
constexpr int    MAX_MEMTABLE_SIZE_PER_TABLE       = 1024 * 1024 * 1;  // 1MB
constexpr int    MAX_SSTABLE_SIZE                  = 1024 * 1024 * 4;  // 4MB
constexpr int    Block_SIZE                        = 1024 * 4;         // 4KB
constexpr int    Block_CACHE_capacity              = 1024 * 4;
constexpr int    Block_CACHE_K                     = 8;
constexpr int    LSM_SST_LEVEL_RATIO               = 4;
constexpr int    bloom_filter_expected_size_       = 65536;
constexpr double bloom_filter_expected_error_rate_ = 0.1;
enum class SkiplistStatus {
  kNormal,
  KFreezing,
  kFrozen,
};

int generateRandom(int begin = 0, int end = 1000);

}  // namespace Global_