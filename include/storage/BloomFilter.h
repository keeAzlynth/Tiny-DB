#pragma once
#include <cstdint>
#include <string_view>
#include <vector>
#include "../core/Global.h"

class BloomFilter {
public:
    BloomFilter(size_t expected_elements = Global_::bloom_filter_expected_size_,
                double false_positive_rate = Global_::bloom_filter_expected_error_rate_);

    // 兼容旧的三参数构造（外部已有调用的地方不用改）
    BloomFilter(size_t expected_elements, double false_positive_rate, size_t num_bits);

    // 热路径全部用 string_view，零分配
    void add(std::string_view key) noexcept;
    bool possibly_contains(std::string_view key) const noexcept;

    void clear() noexcept;

    // 序列化：磁盘格式与旧版完全兼容
    size_t               encode_size() const noexcept;
    size_t               encode_into(uint8_t* dst) const;
    std::vector<uint8_t> encode() const;
    static BloomFilter   decode(const std::vector<uint8_t>& data);

private:
     struct DecodeTag {};
    explicit BloomFilter(DecodeTag) noexcept {}

    // 一次哈希计算出两个独立的 64 位值，供 double hashing 使用
    // 不做任何堆分配
    static std::pair<uint64_t, uint64_t> hash_pair(std::string_view key) noexcept;

    // 位操作：bits_ 已经是字节压缩布局，pos 是 [0, num_bits_) 的位下标
    inline void set_bit(uint64_t pos) noexcept {
        bits_[pos >> 3] |= static_cast<uint8_t>(1u << (pos & 7u));
    }
    inline bool get_bit(uint64_t pos) const noexcept {
        return (bits_[pos >> 3] >> (pos & 7u)) & 1u;
    }

    size_t expected_elements_;
    double false_positive_rate_;
    uint64_t num_bits_;
    uint32_t num_hashes_;
    // 大小 = (num_bits_ + 7) / 8，每个 bit 对应一个过滤器位
    // 这让 encode/decode 变成纯 memcpy，set/get 变成一次内存访问
    std::vector<uint8_t> bits_;
};