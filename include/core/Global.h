#pragma once
#include <random>
#include <string_view>
#include <span>
#include <stdexcept>
namespace Global_ {
constexpr int              FIX_LEVEL   = 5;
constexpr int              MAX_LEVEL   = 12;
constexpr int              NUMS_SHARDS = 8;  // 分片数量
constexpr int              MAX_MEMTABLE_SIZE_PER_TABLE       = 1024ULL * 1024 * 3;  // 3MB
constexpr int              MAX_SSTABLE_SIZE                  = 1024ULL * 1024 * 3;  // 3MB
constexpr int              Block_SIZE                        = 1024ULL * 4;         // 4KB
constexpr int              Block_CACHE_capacity              = 1024ULL*1024 * 256; //256MB
constexpr int              Block_CACHE_K                     = 2;
constexpr int              LSM_SST_LEVEL_RATIO               = 4;
constexpr int              bloom_filter_expected_size_       = 1024ULL*64;
constexpr double           bloom_filter_expected_error_rate_ = 0.01;
constexpr size_t L1_BUDGET_MB =
    10 * NUMS_SHARDS * MAX_MEMTABLE_SIZE_PER_TABLE / (1024ULL * 1024);
constexpr std::string_view WAL_DIR                           = "data/wal";
constexpr bool WAL_SYNC_ON_WRITE = false;  // 改为 false 开启 group-only sync
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
// Global.h — 找到这行改掉
constexpr WalWritePolicy WAL_WRITE_POLICY = WalWritePolicy::kUnordered; // ← 原来是这个
//constexpr WalWritePolicy WAL_WRITE_POLICY = WalWritePolicy::kPipelined;
// ── Write policy ─────────────────────────────────────────────────────────────
//   kPipelined : group leader writes WAL for the whole batch, then releases
//                the write lock so the next group can start WAL while the
//                current group writes MemTable → pipeline.
//   kUnordered : every caller independently acquires the write lock, writes
//                its own entry and fsyncs.  Simpler; good for low concurrency.
int generateRandom(int begin = 0, int end = 1000);

inline uint64_t fast_hash(std::string_view key) {
    uint64_t h = 0xbf58476d1ce4e5b9;
    for (char c : key) {
        h = (h ^ static_cast<uint8_t>(c)) * 0xbf58476d1ce4e5b9;
    }
    // 核心改进：最后再搅动一下，防止低位碰撞
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccd;
    h ^= h >> 33;
return static_cast<size_t>(h & (Global_::NUMS_SHARDS - 1));
}
inline consteval auto make_crc32c_table() noexcept {
  std::array<uint32_t, 256> t{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k) c = (c >> 1) ^ (0x82F63B78u & -(c & 1u));
    t[i] = c;
  }
  return t;
}
inline constexpr auto kCrc32cLookup = make_crc32c_table();

[[nodiscard]] inline constexpr uint32_t crc32c(std::span<const uint8_t> data) noexcept {
  uint32_t crc = ~0u;
  for (auto b : data) crc = (crc >> 8) ^ kCrc32cLookup[(crc ^ b) & 0xFF];
  return ~crc;
}

// LevelDB-style masking: rotate + salt so a CRC of all-zeros differs from 0.
[[nodiscard]] inline constexpr uint32_t mask_crc(uint32_t crc) noexcept {
  return ((crc >> 15) | (crc << 17)) + 0xa282ead8u;
}
[[nodiscard]] inline constexpr uint32_t unmask_crc(uint32_t v) noexcept {
  const uint32_t rot = v - 0xa282ead8u;
  return (rot >> 17) | (rot << 15);
}
// 在公共头文件里定义，使用 inline 防止重复定义报错
template <std::integral T>
inline void write_le(std::vector<uint8_t>& buf, T v) noexcept {
  // 将 v 强制转换为无符号长整型，避免位移时的隐式提升问题
  auto val = static_cast<std::make_unsigned_t<T>>(v);
  for (size_t i = 0; i < sizeof(T); ++i) {
    buf.push_back(static_cast<uint8_t>(val & 0xFF));
    val >>= 8;
  }
}


template <std::integral T>
inline T read_le(std::span<const uint8_t> src, size_t off = 0) {
  if (off + sizeof(T) > src.size()) {
      throw std::runtime_error("Buffer underflow during read_le");
  }
  T v{};
  for (size_t i = 0; i < sizeof(T); ++i) 
    v |= static_cast<T>(src[off + i]) << (i * 8);
  return v;
}

}  // namespace Global_