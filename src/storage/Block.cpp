#include "../../include/storage/Block.h"
#include <spdlog/spdlog.h>
#include "../../include/iterator/BlockIterator.h"
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <print>

class BlockIterator;
Block::Block(std::size_t capacity) : capcity(capacity) {}
Block::Block() : capcity(4096) {
  // 默认构造函数，初始化容量为4096
}

std::vector<uint8_t> Block::encode(bool with_hash) {
  // Data_ + offsets(uint16_t) + num(uint16_t) [+ hash(uint32_t)]
  size_t offsets_bytes = Offset_.size() * sizeof(uint16_t);
  size_t total         = Data_.size() * sizeof(uint8_t) + offsets_bytes + sizeof(uint16_t) +
                 (with_hash ? sizeof(uint32_t) : 0);
  std::vector<uint8_t> encoded(total, 0);

  std::memcpy(encoded.data(), Data_.data(), Data_.size() * sizeof(uint8_t));

  // write offsets as uint16_t (explicit conversion + check)
  size_t off_pos = Data_.size() * sizeof(uint8_t);
  memcpy(encoded.data() + off_pos, Offset_.data(), Offset_.size() * sizeof(uint16_t));

  // write num elements
  size_t   num_pos      = Data_.size() * sizeof(uint8_t) + Offset_.size() * sizeof(uint16_t);
  uint16_t num_elements = Offset_.size();
  memcpy(encoded.data() + num_pos, &num_elements, sizeof(uint16_t));

  // write hash if needed (hash over everything before hash)
  if (with_hash) {
    uint32_t hash_value = std::hash<std::string_view>{}(std::string_view(
        reinterpret_cast<const char*>(encoded.data()), encoded.size() - sizeof(uint32_t)));
    std::memcpy(encoded.data() + encoded.size() - sizeof(uint32_t), &hash_value, sizeof(uint32_t));
  }
  return encoded;
}

std::shared_ptr<Block> Block::decode(const std::vector<uint8_t>& encoded, bool with_hash) {
  // 使用 make_shared 创建对象
  auto block = std::make_shared<Block>();

  // 1. 安全性检查
  if (with_hash && encoded.size() <= sizeof(uint16_t) + sizeof(uint32_t)) {
    spdlog::info(
        "Block::decode(const std::vector<uint8_t>& encoded, bool with_hash) Encoded data too "
        "small");
    return nullptr;
  }

  // 2. 读取元素个数
  uint16_t num_elements;
  size_t   num_elements_pos = encoded.size() - sizeof(uint16_t);
  if (with_hash) {
    num_elements_pos -= sizeof(uint32_t);
    auto     hash_pos = encoded.size() - sizeof(uint32_t);
    uint32_t hash_value;
    memcpy(&hash_value, encoded.data() + hash_pos, sizeof(uint32_t));

    uint32_t compute_hash = std::hash<std::string_view>{}(std::string_view(
        reinterpret_cast<const char*>(encoded.data()), encoded.size() - sizeof(uint32_t)));
    if (hash_value != compute_hash) {
      throw std::runtime_error("Block hash verification failed");
    }
  }
  memcpy(&num_elements, encoded.data() + num_elements_pos, sizeof(uint16_t));

  // 4. 计算各段位置
  size_t offsets_section_start = num_elements_pos - num_elements * sizeof(uint16_t);

  // 5. 读取偏移数组
  block->Offset_.resize(num_elements);
  memcpy(block->Offset_.data(), encoded.data() + offsets_section_start,
         num_elements * sizeof(uint16_t));

  // 6. 复制数据段
  block->Data_.reserve(offsets_section_start);  // 优化内存分配
  block->Data_.assign(encoded.begin(), encoded.begin() + offsets_section_start);

  return block;
}

// safe get_key/get_value/get_tranc_id with bounds checks
std::string Block::get_key(const std::size_t offset) const {
  if (offset > Offset_[Offset_.size() - 1]) {
    spdlog::info(" Block::get_key(const std::size_t offset) {} Invaild offset to much{}", offset,
                 Offset_[Offset_.size() - 1]);
  }
  uint16_t key_len;
  std::memcpy(&key_len, Data_.data() + offset, sizeof(uint16_t));
  return std::string(reinterpret_cast<const char*>(Data_.data() + offset + sizeof(uint16_t)),
                     key_len);
}
std::optional<std::pair<std::string, uint64_t>> Block::get_value(
    const std::size_t offset) const {
  if (offset > Offset_[Offset_.size() - 1]) {
    spdlog::info("Block::get_value(const std::size_t offset) {} Invalid offset too much {}", offset,
                 Offset_[Offset_.size() - 1]);
    return std::nullopt;
  }

  // 获取底层字节数据指针（unsigned char 类型）
  const unsigned char* base = Data_.data();

  // 读取 key_len
  uint16_t key_len;
  std::memcpy(&key_len, base + offset, sizeof(uint16_t));

  // value_len 紧跟在 key 数据之后
  const unsigned char* value_len_ptr = base + offset + sizeof(uint16_t) + key_len;
  uint16_t             value_len;
  std::memcpy(&value_len, value_len_ptr, sizeof(uint16_t));

  // value 数据的起始位置
  const unsigned char* value_start = value_len_ptr + sizeof(uint16_t);

  // tr 紧跟在 value 数据之后
  const unsigned char* tr_ptr = value_start + value_len;
  uint64_t             tr;
  std::memcpy(&tr, tr_ptr, sizeof(uint64_t));

  // 构造 string_view，需要转换为 const char*
  std::string value_view(reinterpret_cast<const char*>(value_start), value_len);
  return std::make_pair(value_view, tr);
}
std::shared_ptr<Block::Entry> Block::get_entry(std::size_t offset) {
  if (offset > Offset_[Offset_.size() - 1]) {
    spdlog::info("Block::get_entry(std::size_t offset) {} Invaild offset to much{}", offset,
                 Offset_[Offset_.size() - 1]);
  }
  auto key            = get_key(offset);
  auto value_tranc_id = get_value(offset);
  return std::make_shared<Block::Entry>(
      Block::Entry{key, std::string(value_tranc_id->first), value_tranc_id->second});
}

std::optional<uint64_t> Block::get_tranc_id(const std::size_t offset) const {
  if (offset > Offset_[Offset_.size() - 1]) {
    spdlog::info("Block::get_tranc_id(const std::size_t offset) {} Invaild offset {}", offset,
                 Offset_[Offset_.size() - 1]);
  }
  uint16_t key_len;
  std::memcpy(&key_len, Data_.data() + offset, sizeof(uint16_t));
  size_t   pos = offset + sizeof(uint16_t) + key_len;
  uint16_t value_len;
  std::memcpy(&value_len, Data_.data() + pos, sizeof(uint16_t));
  pos += sizeof(uint16_t) + value_len;
  uint64_t tr;
  std::memcpy(&tr, Data_.data() + pos, sizeof(uint64_t));
  return tr;
}
std::string Block::get_first_key() {
  if (Offset_.empty()) {
    return std::string();
  }
  return get_key(Offset_[0]);
}

std::optional<std::pair<size_t, size_t>> Block::get_offset_binary(std::string_view key,
                                                                  const uint64_t   tranc_id) {
  if (Offset_.empty()) {
    return std::nullopt;
  }
  // 二分查找
  int left  = 0;
  int right = Offset_.size() - 1;
  while (left <= right) {
    int         mid     = left + (right - left) / 2;
    std::string mid_key = get_key(Offset_[mid]);
    if (mid_key == key) {
      return std::make_pair<size_t, size_t>(Offset_[mid], mid);
    } else if (mid_key < key) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  // 如果没有找到完全匹配的键，返回 std::nullopt
  return std::nullopt;
}

std::optional<std::pair<size_t, size_t>> Block::get_prefix_begin_offset_binary(
    std::string_view key_prefix) {
  if (Offset_.empty())
    return std::nullopt;

  // 优先检查完全匹配（并传入 tranc_id）
  auto exact = get_offset_binary(key_prefix);
  if (exact.has_value()) {
    auto index = exact.value().second;
    while (index != 0 && get_key(Offset_[index - 1]) == key_prefix) {
      index--;
    }
    return std::make_optional<std::pair<size_t, size_t>>(Offset_[index], index);
  }

  // 二分查找插入点（第一个 >= key_prefix）
  int left       = 0;
  int right      = Offset_.size() - 1;
  int result_idx = -1;
  while (left <= right) {
    int         mid     = left + (right - left) / 2;
    std::string mid_key = get_key(Offset_[mid]);
    if (mid_key.starts_with(key_prefix)) {
      result_idx = mid;
      right      = mid - 1;  // 继续查找可能更早的匹配项
    } else if (mid_key < key_prefix) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  if (result_idx != -1) {
    return std::make_pair(Offset_[result_idx], result_idx);
  }
  return std::nullopt;
}

std::optional<std::pair<size_t, size_t>> Block::get_prefix_end_offset_binary(
    std::string_view key_prefix) {
  if (Offset_.empty()) {
    return std::nullopt;
  }
  int         left           = 0;
  int         right          = Offset_.size() - 1;
  int         result_idx     = -1;
  std::string key_prefix_str = std::string(key_prefix);
  std::string key_prefix_end = key_prefix_str + '\xFF';

  while (left <= right) {
    int         mid     = left + (right - left) / 2;
    std::string mid_key = get_key(Offset_[mid]);
    if (mid_key.starts_with(key_prefix)) {
      result_idx = mid;
      left       = mid + 1;  // 继续查找可能更晚的匹配项
    } else if (mid_key < key_prefix_end) {
      left = mid + 1;  // mid+1，避免死循环
    } else {
      right = mid - 1;
    }
  }
  if (result_idx != -1) {
    // 返回第一个不匹配的位置作为范围的结束
    return std::make_pair(Offset_[result_idx], result_idx + 1);
  }
  return std::nullopt;
}

std::vector<std::tuple<std::string, std::string, uint64_t>> Block::get_prefix_tran_id(
    std::string_view key, const uint64_t tranc_id) {
  std::vector<std::tuple<std::string, std::string, uint64_t>> retrieved_data;
  auto result1 = get_prefix_begin_offset_binary(key);
  if (!result1.has_value()) {
    return retrieved_data;
  }
  auto begin = std::make_shared<BlockIterator>(shared_from_this(), result1->second,
                                               get_tranc_id(result1->first).value());
  if (result1.value().second == Offset_.size() - 1) {
    if (begin->get_cur_tranc_id() <= tranc_id) {
      auto entry = begin->getValue();
      retrieved_data.push_back({entry.first, entry.second, begin->get_cur_tranc_id()});
    }
    return retrieved_data;
  }
  auto result2 = get_prefix_end_offset_binary(key);

  // 如果 result2 没有值，说明所有剩余 key 都以前缀开头
  size_t end_idx;
  if (!result2.has_value()) {
    end_idx = Offset_.size();
  } else {
    end_idx = result2->second;
  }

  auto end = std::make_shared<BlockIterator>(
      shared_from_this(), end_idx,
      end_idx < Offset_.size() ? get_tranc_id(result2->first).value()
                               : get_tranc_id(Offset_[Offset_.size() - 1]).value(),
      false);
  while (*begin != *end) {
    if (begin->get_cur_tranc_id() <= tranc_id) {
      auto entry = begin->getValue();
      retrieved_data.push_back({entry.first, entry.second, begin->get_cur_tranc_id()});
    }
    ++(*begin);
  }
  return retrieved_data;
}
std::optional<size_t> Block::get_offset(const std::size_t index) {
  if (index >= Offset_.size()) {
    spdlog::info("Block::get_offset(const std::size_t index) Index out of range");
    return std::nullopt;
  }
  return Offset_[index];
}
size_t Block::get_cur_size() const {
  return Data_.size() + Offset_.size() * sizeof(uint16_t) + sizeof(uint16_t);
}

std::optional<std::pair<std::string, uint64_t>> Block::get_value_binary(std::string_view key) {
  auto idx = get_offset_binary(key);
  if (idx.has_value()) {
    return get_value(idx->first);
  }
  return std::nullopt;
}

bool Block::KeyExists(std::string_view key) {
  auto idx = get_offset_binary(key);
  return idx.has_value();
}

std::pair<std::string, std::string> Block::get_first_and_last_key() {
  if (Offset_.empty()) {
    return {std::string(), std::string()};
  }
  std::string first_key = get_key(Offset_[0]);
  std::string last_key  = get_key(Offset_[Offset_.size() - 1]);
  return {first_key, last_key};
}
bool Block::add_entry(const std::string& key, const std::string& value, const uint64_t tranc_id,
                      bool force_write) {
  if ((!force_write) &&
      (get_cur_size() + key.size() + value.size() + 3 * sizeof(uint16_t) > capcity) &&
      !Offset_.empty()) {
    return false;
  }
  // 计算entry大小：key长度(2B) + key + value长度(2B) + value + tranc_id
  size_t entry_size =
      sizeof(uint16_t) + key.size() + sizeof(uint16_t) + value.size() + sizeof(const uint64_t);
  size_t old_size = Data_.size();
  Data_.resize(old_size + entry_size);

  // 写入key长度
  uint16_t key_len = key.size();
  memcpy(Data_.data() + old_size, &key_len, sizeof(uint16_t));

  // 写入key
  memcpy(Data_.data() + old_size + sizeof(uint16_t), key.data(), key_len);

  // 写入value长度
  uint16_t value_len = value.size();
  memcpy(Data_.data() + old_size + sizeof(uint16_t) + key_len, &value_len, sizeof(uint16_t));

  // 写入value
  memcpy(Data_.data() + old_size + sizeof(uint16_t) + key_len + sizeof(uint16_t), value.data(),
         value_len);
  // 写入tranc_id
  memcpy(Data_.data() + old_size + sizeof(uint16_t) + key_len + sizeof(uint16_t) + value_len,
         &tranc_id, sizeof(const uint64_t));
  // 记录偏移
  Offset_.push_back(old_size);
  return true;
}
bool Block::is_empty() const {
  return Data_.empty() && Offset_.empty();
}
void Block::print_debug() const {
  if (is_empty()) {
    return;
  }
  for (size_t i = 0; i < Offset_.size(); i++) {
    uint16_t key_len;
    std::memcpy(&key_len, Data_.data() + Offset_[i], sizeof(uint16_t));
    auto key = std::string(
        reinterpret_cast<const char*>(Data_.data() + Offset_[i] + sizeof(uint16_t)), key_len);
    size_t   pos = Offset_[i] + sizeof(uint16_t) + key_len;
    uint16_t value_len;
    std::memcpy(&value_len, Data_.data() + pos, sizeof(uint16_t));
    pos += sizeof(uint16_t);
    auto value = std::string(reinterpret_cast<const char*>(Data_.data() + pos), value_len);
    pos += value_len;
    uint64_t tr;
    std::memcpy(&tr, Data_.data() + pos, sizeof(uint64_t));
    std::print("Block Entry {}: key={}, value={}, tranc_id={}\n", i, key, value, tr);
  }
}

BlockIterator Block::get_iterator(std::string_view key, const uint64_t tranc_id) {
  auto idx = get_offset_binary(key, tranc_id);
  if (!idx.has_value()) {
    return end();
  }
  return BlockIterator(shared_from_this(), idx->second, tranc_id);
}
BlockIterator Block::begin() {
  return BlockIterator(shared_from_this(), 0, 0);
}
BlockIterator Block::end() {
  return BlockIterator(shared_from_this(), Offset_.size(), 0);
}

std::optional<std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::get_prefix_iterator(std::string key) {
  auto result1 = get_prefix_begin_offset_binary(key);
  if (!result1.has_value()) {
    return std::nullopt;
  }
  auto begin = std::make_shared<BlockIterator>(shared_from_this(), result1->second, 0, false);
  if (result1->second == Offset_.size() - 1) {
    auto end =
        std::make_shared<BlockIterator>(shared_from_this(), Offset_.size(),
                                        get_tranc_id(Offset_[Offset_.size() - 1]).value(), false);
    return std::make_pair(begin, end);
  }

  auto   result2 = get_prefix_end_offset_binary(key);
  size_t end_idx;

  // 如果 result2 没有值，说明所有剩余key都以前缀开头
  if (!result2.has_value()) {
    end_idx = Offset_.size();
  } else {
    end_idx = result2->second;
  }

  auto end = std::make_shared<BlockIterator>(
      shared_from_this(), end_idx,
      end_idx < Offset_.size() ? get_tranc_id(result2->first).value()
                               : get_tranc_id(Offset_[Offset_.size() - 1]).value(),
      false);
  return std::make_pair(begin, end);
}