
#include "../../include/storage/BloomFilter.h"
#include <cstring>
#include <stdexcept>
#include <string>
#include <cmath>

BloomFilter::BloomFilter(){};

// 构造函数，初始化布隆过滤器
// expected_elements: 预期插入的元素数量
// false_positive_rate: 允许的假阳性率
BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate)
    : expected_elements_(expected_elements), false_positive_rate_(false_positive_rate) {
  // 计算布隆过滤器的位数组大小
  double m = -static_cast<double>(expected_elements) * std::log(false_positive_rate) /
             std::pow(std::log(2), 2);
  //-n*ln(p)/[ln(2)^2]：这是布隆过滤器理论中的最优位数公式.
  num_bits_ = static_cast<size_t>(std::ceil(m));
  // 向上取整,实际存储时，位数只能用整数，所以向上取整，确保不会小于理论值.
  //  计算哈希函数的数量
  num_hashes_ = static_cast<size_t>(std::ceil(m / expected_elements * std::log(2)));
  // 这是布隆过滤器理论中的最优哈希函数数量公式 k = (m/n) * ln(2).

  // 初始化位数组
  bits_.resize(num_bits_, false);
}

void BloomFilter::add(const std::string& key) {
  // 对每个哈希函数计算哈希值，并将对应位置的位设置为true
  for (size_t i = 0; i < num_hashes_; ++i) {
    bits_[hash(key, i)] = true;
  }
}

//  如果key可能存在于布隆过滤器中，返回true；否则返回false
bool BloomFilter::possibly_contains(std::string_view key) const {
  // 对每个哈希函数计算哈希值，检查对应位置的位是否都为true
  std::string key_str(key);
  for (size_t i = 0; i < num_hashes_; ++i) {
    auto bit_idx = hash(key_str, i);
    if (!bits_[bit_idx]) {
      return false;
    }
  }
  return true;
}

// 清空布隆过滤器
void BloomFilter::clear() {
  bits_.assign(bits_.size(), false);
}

std::pair<size_t, size_t> BloomFilter::hash_pair(const std::string& key) const {
  size_t h1 = hasher_(key);
  // 使用更好的第二个哈希函数
  size_t h2 = hasher_(key + std::to_string(h1)) | 1;  // 确保 h2 为奇数
  return {h1, h2};
}

size_t BloomFilter::hash(const std::string& key, size_t idx) const {
  auto [h1, h2] = hash_pair(key);
  return (h1 + idx * h2) % num_bits_;
}
size_t BloomFilter::encode_size() const {
  return sizeof(expected_elements_)
       + sizeof(false_positive_rate_)
       + sizeof(num_bits_)
       + sizeof(num_hashes_)
       + (num_bits_ + 7) / 8;
}

void BloomFilter::encode_bits_fast(uint8_t* ptr, size_t num_bytes) const {
  const size_t full_bytes = num_bits_ / 8;
  const size_t rem_bits   = num_bits_ % 8;

  for (size_t i = 0; i < full_bytes; ++i) {
    const size_t b = i * 8;
    *ptr++ = (bits_[b]     ? 0x01 : 0)
           | (bits_[b + 1] ? 0x02 : 0)
           | (bits_[b + 2] ? 0x04 : 0)
           | (bits_[b + 3] ? 0x08 : 0)
           | (bits_[b + 4] ? 0x10 : 0)
           | (bits_[b + 5] ? 0x20 : 0)
           | (bits_[b + 6] ? 0x40 : 0)
           | (bits_[b + 7] ? 0x80 : 0);
  }

  if (rem_bits > 0) {
    uint8_t      byte = 0;
    const size_t b    = full_bytes * 8;
    for (size_t j = 0; j < rem_bits; ++j) {
      if (bits_[b + j]) byte |= (1u << j);
    }
    *ptr = byte;
  }
}

void BloomFilter::decode_bits_fast(const uint8_t* ptr, size_t num_bytes) {
  const size_t full_bytes = num_bits_ / 8;
  const size_t rem_bits   = num_bits_ % 8;

  for (size_t i = 0; i < full_bytes; ++i) {
    const uint8_t byte = *ptr++;
    const size_t  b    = i * 8;
    bits_[b]     = (byte & 0x01) != 0;
    bits_[b + 1] = (byte & 0x02) != 0;
    bits_[b + 2] = (byte & 0x04) != 0;
    bits_[b + 3] = (byte & 0x08) != 0;
    bits_[b + 4] = (byte & 0x10) != 0;
    bits_[b + 5] = (byte & 0x20) != 0;
    bits_[b + 6] = (byte & 0x40) != 0;
    bits_[b + 7] = (byte & 0x80) != 0;
  }

  if (rem_bits > 0) {
    const uint8_t byte = *ptr;
    const size_t  b    = full_bytes * 8;
    for (size_t j = 0; j < rem_bits; ++j) {
      bits_[b + j] = (byte & (1u << j)) != 0;
    }
  }
}

size_t BloomFilter::encode_into(uint8_t* dst) const {
  uint8_t* ptr = dst;

  std::memcpy(ptr, &expected_elements_,   sizeof(expected_elements_));   ptr += sizeof(expected_elements_);
  std::memcpy(ptr, &false_positive_rate_, sizeof(false_positive_rate_)); ptr += sizeof(false_positive_rate_);
  std::memcpy(ptr, &num_bits_,            sizeof(num_bits_));            ptr += sizeof(num_bits_);
  std::memcpy(ptr, &num_hashes_,          sizeof(num_hashes_));          ptr += sizeof(num_hashes_);

  const size_t num_bytes = (num_bits_ + 7) / 8;
  encode_bits_fast(ptr, num_bytes);

  return static_cast<size_t>(ptr - dst) + num_bytes;
}

std::vector<uint8_t> BloomFilter::encode() const {
  std::vector<uint8_t> buf(encode_size());
  encode_into(buf.data());
  return buf;
}

BloomFilter BloomFilter::decode(const std::vector<uint8_t>& data) {
  if (data.size() < sizeof(size_t) * 3 + sizeof(double)) {
    throw std::runtime_error("Invalid data size");
  }

  const auto* ptr = data.data();
  BloomFilter bf;

  std::memcpy(&bf.expected_elements_,   ptr, sizeof(bf.expected_elements_));   ptr += sizeof(bf.expected_elements_);
  std::memcpy(&bf.false_positive_rate_, ptr, sizeof(bf.false_positive_rate_)); ptr += sizeof(bf.false_positive_rate_);
  std::memcpy(&bf.num_bits_,            ptr, sizeof(bf.num_bits_));            ptr += sizeof(bf.num_bits_);
  std::memcpy(&bf.num_hashes_,          ptr, sizeof(bf.num_hashes_));          ptr += sizeof(bf.num_hashes_);

  const size_t expected_bytes = (bf.num_bits_ + 7) / 8;
  const size_t header_size    = ptr - data.data();
  if (data.size() != header_size + expected_bytes) {
    throw std::invalid_argument("Data size mismatch");
  }

  bf.bits_.resize(bf.num_bits_);
  bf.decode_bits_fast(ptr, expected_bytes);

  return bf;
}