#include "../../include/storage/BloomFilter.h"
#include <cassert>
#include <cmath>
#include <cstring>
#include <stdexcept>

// ─── 内联哈希实现（无外部依赖）────────────────────────────────────────────
// 基于 MurmurHash3 finalizer + Knuth 乘法哈希的 double hashing。
namespace {

// MurmurHash3 的 64 位 finalizer（avalanche 混合）
inline uint64_t fmix64(uint64_t k) noexcept {
    k ^= k >> 33;
    k *= UINT64_C(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k *= UINT64_C(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    return k;
}

// 一次扫描产生两个独立的 64 位哈希，供 double hashing 使用：
//   bit_pos(i) = (h1 + i * h2) % num_bits
// 与旧实现相比：
//   - 无 string 构造，无 to_string，无堆分配
//   - 处理任意长度键（8 字节块 + 尾部余量）
std::pair<uint64_t, uint64_t> hash128(std::string_view key) noexcept {
    constexpr uint64_t seed1 = UINT64_C(0x9e3779b97f4a7c15); // 黄金比例
    constexpr uint64_t seed2 = UINT64_C(0x6c62272e07bb0142); // FNV 偏移

    uint64_t h1 = seed1 ^ (key.size() * UINT64_C(0xbf58476d1ce4e5b9));
    uint64_t h2 = seed2 ^ (key.size() * UINT64_C(0x94d049bb133111eb));

    const auto* data = reinterpret_cast<const uint8_t*>(key.data());
    const size_t nblocks = key.size() / 8;

    // 每次处理 8 字节
    for (size_t i = 0; i < nblocks; ++i) {
        uint64_t block;
        std::memcpy(&block, data + i * 8, 8);
        h1 = fmix64(h1 ^ block);
        h2 = fmix64(h2 ^ block);
    }

    // 处理尾部 1–7 字节
    const size_t tail_len = key.size() & 7u;
    if (tail_len > 0) {
        uint64_t tail = 0;
        std::memcpy(&tail, data + nblocks * 8, tail_len);
        h1 ^= fmix64(tail);
        h2 ^= fmix64(tail ^ UINT64_C(0xdeadbeefcafe1234));
    }

    // 最终混合：确保两个值相互独立
    h1 ^= h2;
    h2 ^= h1;
    return {fmix64(h1), fmix64(h2) | 1u}; // h2 保持奇数，确保 double hashing 覆盖所有桶
}

} // namespace

// ─── 构造函数 ─────────────────────────────────────────────────────────────

BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate)
    : expected_elements_(expected_elements), false_positive_rate_(false_positive_rate) {
    // 最优位数公式：m = -n * ln(p) / (ln2)^2
    const double m = -static_cast<double>(expected_elements) * std::log(false_positive_rate) /
                     (std::log(2.0) * std::log(2.0));
    num_bits_   = static_cast<uint64_t>(std::ceil(m));
    // 最优哈希数：k = (m/n) * ln2
    num_hashes_ = static_cast<uint32_t>(std::ceil((m / expected_elements) * std::log(2.0)));

    // 核心改动：bits_ 是字节压缩数组，大小 = ceil(num_bits_ / 8)
    bits_.assign((num_bits_ + 7u) / 8u, 0u);
}

BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate, size_t num_bits)
    : expected_elements_(expected_elements),
      false_positive_rate_(false_positive_rate),
      num_bits_(static_cast<uint64_t>(num_bits)) {
    num_hashes_ = static_cast<uint32_t>(
        std::ceil((static_cast<double>(num_bits) / expected_elements) * std::log(2.0)));
    bits_.assign((num_bits_ + 7u) / 8u, 0u);
}

std::pair<uint64_t, uint64_t> BloomFilter::hash_pair(std::string_view key) noexcept {
    return hash128(key);
}

void BloomFilter::add(std::string_view key) noexcept {
    auto [h1, h2] = hash128(key);
    for (uint32_t i = 0; i < num_hashes_; ++i) {
        // double hashing：(h1 + i * h2) % m
        // 这里用 128 位乘法避免模运算的分支预测代价（与 RocksDB 做法相似）
        const uint64_t pos = (h1 + static_cast<uint64_t>(i) * h2) % num_bits_;
        set_bit(pos);
    }
}

bool BloomFilter::possibly_contains(std::string_view key) const noexcept {
    auto [h1, h2] = hash128(key);
    for (uint32_t i = 0; i < num_hashes_; ++i) {
        const uint64_t pos = (h1 + static_cast<uint64_t>(i) * h2) % num_bits_;
        if (!get_bit(pos)) {
            return false; // 确定不存在，立即返回
        }
    }
    return true; // 可能存在（有假阳性概率）
}

void BloomFilter::clear() noexcept {
    std::fill(bits_.begin(), bits_.end(), 0u);
}

// ─── 序列化（磁盘格式）─────────────────────────────────────
// 布局：[expected_elements_ : size_t][false_positive_rate_ : double]
//        [num_bits_ : size_t][num_hashes_ : size_t][bit_data : N bytes]

size_t BloomFilter::encode_size() const noexcept {
    return sizeof(expected_elements_)    // size_t
         + sizeof(false_positive_rate_)  // double
         + sizeof(num_bits_)             // uint64_t（兼容旧 size_t，在 64 位系统上等价）
         + sizeof(num_hashes_)           // uint32_t（注意：旧版是 size_t，见下方注释）
         + bits_.size();                 // ceil(num_bits_ / 8) 字节
}

size_t BloomFilter::encode_into(uint8_t* dst) const {
    uint8_t* ptr = dst;

    // Header
    std::memcpy(ptr, &expected_elements_, sizeof(expected_elements_)); ptr += sizeof(expected_elements_);
    std::memcpy(ptr, &false_positive_rate_, sizeof(false_positive_rate_)); ptr += sizeof(false_positive_rate_);
    std::memcpy(ptr, &num_bits_, sizeof(num_bits_));   ptr += sizeof(num_bits_);
    std::memcpy(ptr, &num_hashes_, sizeof(num_hashes_)); ptr += sizeof(num_hashes_);

    // 位数组：bits_ 已经是字节压缩格式，直接 memcpy，O(n/8) 而非旧版的 O(n) 位操作
    std::memcpy(ptr, bits_.data(), bits_.size());
    ptr += bits_.size();

    return static_cast<size_t>(ptr - dst);
}

std::vector<uint8_t> BloomFilter::encode() const {
    std::vector<uint8_t> buf(encode_size());
    encode_into(buf.data());
    return buf;
}

BloomFilter BloomFilter::decode(const std::vector<uint8_t>& data) {
    // 最小 header 大小校验
    constexpr size_t kHeaderSize = sizeof(size_t)   // expected_elements_
                                 + sizeof(double)    // false_positive_rate_
                                 + sizeof(uint64_t)  // num_bits_
                                 + sizeof(uint32_t); // num_hashes_
    if (data.size() < kHeaderSize) {
        throw std::runtime_error("BloomFilter::decode: data too small");
    }
 BloomFilter bf(DecodeTag{}); 
    const uint8_t* ptr = data.data();

    std::memcpy(&bf.expected_elements_, ptr, sizeof(bf.expected_elements_)); ptr += sizeof(bf.expected_elements_);
    std::memcpy(&bf.false_positive_rate_, ptr, sizeof(bf.false_positive_rate_)); ptr += sizeof(bf.false_positive_rate_);
    std::memcpy(&bf.num_bits_, ptr, sizeof(bf.num_bits_));   ptr += sizeof(bf.num_bits_);
    std::memcpy(&bf.num_hashes_, ptr, sizeof(bf.num_hashes_)); ptr += sizeof(bf.num_hashes_);

    const size_t expected_bytes = (bf.num_bits_ + 7u) / 8u;
    const size_t consumed       = static_cast<size_t>(ptr - data.data());

    if (data.size() != consumed + expected_bytes) {
        throw std::invalid_argument(
            "BloomFilter::decode: data size mismatch, expected " +
            std::to_string(consumed + expected_bytes) +
            " got " + std::to_string(data.size()));
    }

    // 直接 memcpy 到 uint8_t 位压缩数组
    bf.bits_.resize(expected_bytes);
    std::memcpy(bf.bits_.data(), ptr, expected_bytes);

    return bf;
}