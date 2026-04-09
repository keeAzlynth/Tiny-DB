#pragma once
#include <random>
#include <string_view>
namespace Global_ {
constexpr int              FIX_LEVEL                         = 4;
constexpr int              MAX_LEVEL                         = 12;
constexpr int              MAX_MEMTABLE_SIZE_PER_TABLE       = 1024 * 1024 * 1;  // 1MB
constexpr int              MAX_SSTABLE_SIZE                  = 1024 * 1024 * 2;  // 2MB
constexpr int              Block_SIZE                        = 1024 * 4;         // 4KB
constexpr int              Block_CACHE_capacity              = 1024 * 64;
constexpr int              Block_CACHE_K                     = 2;
constexpr int              LSM_SST_LEVEL_RATIO               = 4;
constexpr int              bloom_filter_expected_size_       = 65536;
constexpr double           bloom_filter_expected_error_rate_ = 0.1;
constexpr std::string_view WAL_DIR                           = "data/wal";
constexpr int              WAL_BLOCK_SIZE       = 1024 * 32;         // 32 KB  (already present)
constexpr uint64_t         WAL_FILE_LIMIT       = 1024 * 1024 * 64;  // 64 MB per WAL file
constexpr uint64_t         WAL_CLEAN_INTERVAL_S = 30;                // cleaner cadence (seconds)
enum class WalWritePolicy : uint8_t {
  kPipelined,
  kUnordered,
};
enum class SkiplistStatus {
  kNormal,
  KFreezing,
  kFrozen,
};

constexpr WalWritePolicy WAL_WRITE_POLICY = WalWritePolicy::kPipelined;
// ── Write policy ─────────────────────────────────────────────────────────────
//   kPipelined : group leader writes WAL for the whole batch, then releases
//                the write lock so the next group can start WAL while the
//                current group writes MemTable → pipeline.
//   kUnordered : every caller independently acquires the write lock, writes
//                its own entry and fsyncs.  Simpler; good for low concurrency.
int generateRandom(int begin = 0, int end = 1000);

}  // namespace Global_