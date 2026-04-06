#pragma once
#include <vector>
#include <string>
#include <cstdint>

class BloomFilter {
 public:
  // 构造函数，初始化布隆过滤器
  // expected_elements: 预期插入的元素数量
  // false_positive_rate: 允许的假阳性率
  BloomFilter();
  BloomFilter(size_t expected_elements, double false_positive_rate);

  BloomFilter(size_t expected_elements, double false_positive_rate, size_t num_bits);

  void add(const std::string& key);

  // 如果key可能存在于布隆过滤器中，返回true；否则返回false
  bool possibly_contains(std::string_view key) const;

  // 清空布隆过滤器
  void clear();
size_t encode_size() const;
size_t encode_into(uint8_t* dst) const;
  void encode_bits_fast(uint8_t* ptr, size_t num_bytes) const;
  void decode_bits_fast(const uint8_t* ptr, size_t num_bytes);

  std::vector<uint8_t> encode() const;
  static BloomFilter   decode(const std::vector<uint8_t>& data);

 private:
  std::pair<size_t, size_t> hash_pair(const std::string& key) const;
  size_t                    hash(const std::string& key, size_t idx) const;
  template <typename T>
  void write_value(std::vector<uint8_t>& data, const T& value) const {
    const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&value);
    data.insert(data.end(), bytes, bytes + sizeof(T));
  }
  std::hash<std::string> hasher_;
  // 布隆过滤器的位数组大小
  size_t expected_elements_;
  // 允许的假阳性率
  double false_positive_rate_;
  size_t num_bits_;
  // 哈希函数的数量
  size_t num_hashes_;
  // 布隆过滤器的位数组
  std::vector<bool> bits_;
};