#include "../../include/storage/Sstable.h"
#include "../../include/iterator/SstableIterator.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <optional>
#include <print>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

void Sstable::del_sst() {
  file_obj.del_file();
}
std::shared_ptr<Sstable> Sstable::open(size_t sst_id, FileObj file_obj_,
                                       std::shared_ptr<BlockCache> block_cache) {
  auto sst         = std::make_shared<Sstable>();
  sst->sst_id      = sst_id;
  sst->file_obj    = std::move(file_obj_);
  sst->block_cache = block_cache;
  size_t file_size = sst->file_obj.size();
  // 读取文件末尾的元数据块
  if (file_size < sizeof(uint64_t) * 2 + sizeof(uint32_t) * 2) {
    spdlog::info(
        "Sstable::open(size_t sst_id, FileObj file_obj_,std::shared_ptr<BlockCache> block_cache) "
        "Invalid SST file: too small");
    return sst;
  }

  // 0. 读取最大和最小的事务id
  auto max_tranc_id = sst->file_obj.read_to_slice(file_size - sizeof(uint64_t), sizeof(uint64_t));
  memcpy(&sst->max_tranc_id, max_tranc_id.data(), sizeof(uint64_t));

  auto min_tranc_id =
      sst->file_obj.read_to_slice(file_size - sizeof(uint64_t) * 2, sizeof(uint64_t));
  memcpy(&sst->min_tranc_id, min_tranc_id.data(), sizeof(uint64_t));

  // 1. 读取元数据块的偏移量, 最后8字节: 2个 uint32_t,
  // 分别是 meta 和 bloom 的 offset

  auto bloom_offset_bytes = sst->file_obj.read_to_slice(
      file_size - sizeof(uint64_t) * 2 - sizeof(uint32_t), sizeof(uint32_t));
  memcpy(&sst->bloom_offset, bloom_offset_bytes.data(), sizeof(uint32_t));

  auto meta_offset_bytes = sst->file_obj.read_to_slice(
      file_size - sizeof(uint64_t) * 2 - sizeof(uint32_t) * 2, sizeof(uint32_t));
  memcpy(&sst->meta_block_offset, meta_offset_bytes.data(), sizeof(uint32_t));

  // 2. 读取 bloom filter
  uint32_t bloom_size = file_size - sizeof(uint64_t) * 2 - sst->bloom_offset - sizeof(uint32_t) * 2;
  auto     bloom_bytes = sst->file_obj.read_to_slice(sst->bloom_offset, bloom_size);

  auto bloom        = BloomFilter::decode(bloom_bytes);
  sst->bloom_filter = std::make_shared<BloomFilter>(std::move(bloom));

  // 3. 读取并解码元数据块
  uint32_t meta_size  = sst->bloom_offset - sst->meta_block_offset;
  auto     meta_bytes = sst->file_obj.read_to_slice(sst->meta_block_offset, meta_size);
  sst->block_metas    = BlockMeta::decode_meta_from_slice(meta_bytes);

  // 4. 设置首尾key
  if (!sst->block_metas.empty()) {
    sst->first_key = sst->block_metas.front().first_key_;
    sst->last_key  = sst->block_metas.back().last_key_;
  }

  return sst;
}

std::shared_ptr<Sstable> Sstable::create_sst_with_meta_only(
    size_t sst_id, size_t file_size, const std::string& first_key, const std::string& last_key,
    std::shared_ptr<BlockCache> block_cache) {
  auto sst = std::make_shared<Sstable>();
  sst->file_obj.set_size(file_size);
  sst->sst_id            = sst_id;
  sst->first_key         = first_key;
  sst->last_key          = last_key;
  sst->meta_block_offset = 0;
  sst->block_cache       = block_cache;
  return sst;
}

std::shared_ptr<Block> Sstable::read_block(size_t block_idx) {
  if (!is_block_index_vaild(block_idx)) {
    spdlog::info("Sstable::read_block(size_t block_idx) Block index out of range {}",
                 block_metas.size());
    return nullptr;
  }

  // 先从缓存中查找
  if (block_cache != nullptr) {
    auto cache_ptr = block_cache->get(sst_id, block_idx);
    if (cache_ptr != nullptr) {
      return cache_ptr;
    }
  } else {
    spdlog::info("Sstable::read_block(size_t block_idx) Block cache not set");
  }

  const auto& meta = block_metas[block_idx];
  size_t      block_size;

  // 计算block大小
  if (block_idx == block_metas.size() - 1) {
    block_size = meta_block_offset - meta.offset_;
  } else {
    block_size = block_metas[block_idx + 1].offset_ - meta.offset_;
  }

  // 读取block数据
  auto block_data = file_obj.read_to_slice(meta.offset_, block_size);
  auto block_res  = Block::decode(block_data);

  block_cache->put(sst_id, block_idx, block_res);
  return block_res;
}

std::optional<size_t> Sstable::find_block_idx(std::string_view key, bool is_prefix) {
  if (!is_prefix) {
    // 精确查询：先在布隆过滤器判断key是否存在
    if (bloom_filter != nullptr && !bloom_filter->possibly_contains(key)) {
      return std::nullopt;
    }

    // 二分查找找到包含该key的块
    size_t left  = 0;
    size_t right = block_metas.size();

    while (left < right) {
      size_t      mid  = left + (right - left) / 2;
      const auto& meta = block_metas[mid];
      if (key < meta.first_key_) {
        right = mid;
      } else if (key > meta.last_key_) {
        left = mid + 1;
      } else {
        return mid;  // key在这个块的范围内
      }
    }
    return std::nullopt;
  } else {
    // 前缀查询：找到第一个块，其中last_key >= key_prefix
    // 这样该块可能包含以key_prefix开头的keys
    size_t                left  = 0;
    size_t                right = block_metas.size();
    std::optional<size_t> result;

    while (left < right) {
      size_t      mid  = left + (right - left) / 2;
      const auto& meta = block_metas[mid];
      if (meta.last_key_ >= key) {
        // 这个块可能包含前缀匹配的keys
        result = mid;
        right  = mid;  // 继续查找更早的块
      } else {
        left = mid + 1;
      }
    }
    return result;
  }
}
std::vector<std::shared_ptr<Block>> Sstable::find_block_range(std::string_view key_prefix) {
  std::vector<std::shared_ptr<Block>> result;
  if ((key_prefix < first_key && !first_key.starts_with(key_prefix)) || key_prefix > last_key) {
    return result;
  }

  auto res1 = find_block_idx(key_prefix, true);
  if (!res1.has_value())
    return result;

  result.push_back(read_block(res1.value()));
  std::string key_prefix_end = std::string(key_prefix) + '\xFF';
  for (int index = res1.value() + 1; index < (int) block_metas.size(); index++) {
    // block的first_key还在前缀范围内，继续收集
    if (block_metas[index].first_key_.starts_with(key_prefix) ||
        block_metas[index].first_key_ < key_prefix_end) {
      result.push_back(read_block(index));
    } else {
      break;  // ← break放在这里，超出范围才停止
    }
  }

  return result;
}
size_t Sstable::num_blocks() const {
  return block_metas.size();
}

size_t Sstable::get_sst_size() const {
  return file_obj.size();
}

size_t Sstable::get_sst_id() const {
  return sst_id;
}

std::string Sstable::get_first_key() const {
  return first_key;
}

std::string Sstable::get_last_key() const {
  return last_key;
}

bool Sstable::is_block_index_vaild(size_t block_idx) const {
  return block_idx < block_metas.size() ? true : false;
}
std::optional<std::pair<std::string, uint64_t>> Sstable::KeyExists(std::string_view key,
                                                                        uint64_t         tranc_id) {
  if (key < first_key || key > last_key) {
    return std::nullopt;
  }
  auto block_idx_opt = find_block_idx(key);
  if (!block_idx_opt.has_value()) {
    return std::nullopt;
  }
  auto block = read_block(block_idx_opt.value());
  auto res   = block->get_value_binary(key);
  if (res.has_value() && res->second <= tranc_id) {
    return res;
  }
  return std::nullopt;
}
SstIterator Sstable::get_Iterator(std::string_view key, uint64_t tranc_id, bool is_prefix) {
  if (!is_prefix) {
    if (key < first_key || key > last_key) {
      return end();
    }
    // 在布隆过滤器判断key是否存在
    if (bloom_filter != nullptr && !bloom_filter->possibly_contains(key)) {
      return end();
    }
    return SstIterator(shared_from_this(), std::string(key), tranc_id);
  }
  if ((key < first_key && !first_key.starts_with(key)) || key > last_key) {
    return end();
  }
  if (first_key.starts_with(key)) {
    return begin(tranc_id);
  }
  return SstIterator(shared_from_this(), std::string(key), tranc_id, is_prefix);
}

SstIterator Sstable::current_Iterator(size_t block_idx, uint64_t tranc_id) {
  if (block_idx >= block_metas.size()) {
    spdlog::info(
        "Sstable::current_Iterator(size_t block_idx, uint64_t tranc_id)Block index out of range");
  }
  return SstIterator(shared_from_this(), block_idx, std::string(), tranc_id);
}
SstIterator Sstable::begin(uint64_t tranc_id) {
  return SstIterator(shared_from_this(), tranc_id);
}

SstIterator Sstable::end() {
  SstIterator res(shared_from_this(), 0);
  res.m_block_idx = block_metas.size();
  res.m_block_it  = nullptr;
  return res;
}

std::pair<uint64_t, uint64_t> Sstable::get_tranc_id_range() const {
  return std::make_pair(min_tranc_id, max_tranc_id);
}

std::vector<std::tuple<std::string, std::string, uint64_t>> Sstable::get_prefix_range(
    std::string_view key, uint64_t tranc_id) {
  std::vector<std::tuple<std::string, std::string, uint64_t>> res;
  if (key > last_key || (key < first_key && !first_key.starts_with(key))) {
    spdlog::info(
        "Sstable::get_prefix_range(const std::string& key,uint64_t tranc_id) Prefix out of range: "
        "{} not in [{}, {}]\n",
        key, first_key, last_key);
    return res;
  }

  auto result = find_block_range(key);
  if (result.empty()) {
    spdlog::info(
        "Sstable::get_prefix_range(const std::string& key,uint64_t tranc_id) No blocks found for "
        "prefix: {}\n",
        key);
    return res;
  }
  for (auto& re : result) {
    auto range_res = re->get_prefix_tran_id(key, tranc_id);
    std::ranges::move(range_res.begin(), range_res.end(), std::back_inserter(res));
  }
  return res;
}
Sstbuild::Sstbuild(size_t block_size, bool has_bloom) : block_(block_size) {
  // 初始化第一个block
  if (has_bloom) {
    bloom_filter = std::make_shared<BloomFilter>(Global_::bloom_filter_expected_size_,
                                                 Global_::bloom_filter_expected_error_rate_);
  }
  clean();
}

void Sstbuild::clean() {
  block_metas.clear();
  data.clear();
  first_key.clear();
  last_key.clear();
}
void Sstbuild::add(const std::string& key, const std::string& value, uint64_t tranc_id) {
  // 记录第一个key
  if (first_key.empty()) {
    first_key = key;
  }

  // 在布隆过滤器中添加key
  if (bloom_filter != nullptr) {
    bloom_filter->add(key);
  }

  bool force_write = key == last_key;

  // 记录事务id范围
  max_tranc_id = std::max(max_tranc_id, tranc_id);
  min_tranc_id = std::min(min_tranc_id, tranc_id);

  if (block_.add_entry(key, value, tranc_id, force_write)) {
    // block 插入成功
    last_key = key;
    return;
  }

  // block 满了，需要 finish_block 并创建新 block
  finish_block();

  // 将条目添加到新 block 中（必须成功，否则报错）
  if (!block_.add_entry(key, value, tranc_id)) {
    throw std::runtime_error("Failed to add entry to new block (entry too large?)");
  }

  first_key = key;
  last_key  = key;
}

void Sstbuild::finish_block() {
  // 只有当 block 不为空时才编码并保存
  if (block_.is_empty()) {
    spdlog::info("DBG finish_block: block is empty, skipping");
    return;
  }

  auto old_block     = std::move(block_);
  block_             = Block(Global_::Block_SIZE);  // 创建新 block
  auto encoded_block = old_block.encode();

  if (encoded_block.empty()) {
    spdlog::info("ERROR: encoded_block is empty!");
    throw std::runtime_error("Block encode returned empty data");
  }

  // 记录插入前的起始偏移
  size_t start_offset = data.size();

  // 添加到 data
  data.insert(data.end(), encoded_block.begin(), encoded_block.end());

  // 保存元数据
  block_metas.emplace_back(first_key, last_key, start_offset);
}

size_t Sstbuild::estimated_size() const {
  return data.size();
}

std::shared_ptr<Sstable> Sstbuild::build(std::shared_ptr<BlockCache> block_cache,
                                         const std::string& path, size_t sst_id) {
  if (!block_.is_empty()) {
    finish_block();
  }

  if (block_metas.empty()) {
    spdlog::info("Sstbuild::build: Cannot build empty SST");
    return nullptr;
  }

  // ── 1. 预算各段大小，一次性分配 ──────────────────────────────
  std::vector<uint8_t> meta_block  = BlockMeta::encode_meta_to_slice(block_metas);
  const size_t         bf_size     = (bloom_filter != nullptr) ? bloom_filter->encode_size() : 0;
  const size_t         footer_size = sizeof(uint32_t) * 2 + sizeof(uint64_t) * 2;

  const uint32_t meta_offset  = static_cast<uint32_t>(data.size());
  const uint32_t bloom_offset = static_cast<uint32_t>(meta_offset + meta_block.size());

  const size_t total_size = data.size() + meta_block.size() + bf_size + footer_size;

  std::vector<uint8_t> file_content = std::move(data);
  file_content.resize(total_size);

  uint8_t* base = file_content.data();
  uint8_t* ptr  = base + meta_offset;

  // ── 3. 写 meta block ─────────────────────────────────────────
  std::memcpy(ptr, meta_block.data(), meta_block.size());
  ptr += meta_block.size();

  // ── 4. 写 bloom filter（原地编码，零拷贝）────────────────────
  if (bloom_filter != nullptr) {
    bloom_filter->encode_into(ptr);
    ptr += bf_size;
  }

  // ── 5. 写 footer ──────────────────────────────────────────────
  std::memcpy(ptr, &meta_offset, sizeof(uint32_t));
  ptr += sizeof(uint32_t);
  std::memcpy(ptr, &bloom_offset, sizeof(uint32_t));
  ptr += sizeof(uint32_t);
  std::memcpy(ptr, &min_tranc_id, sizeof(uint64_t));
  ptr += sizeof(uint64_t);
  std::memcpy(ptr, &max_tranc_id, sizeof(uint64_t));

  // ── 6. 写文件 ────────────────────────────────────────────────
  FileObj file = FileObj::create_and_write(path, file_content);

  auto res               = std::make_shared<Sstable>();
  res->sst_id            = sst_id;
  res->file_obj          = std::move(file);
  res->first_key         = block_metas.front().first_key_;
  res->last_key          = block_metas.back().last_key_;
  res->meta_block_offset = meta_offset;
  res->bloom_filter      = bloom_filter;
  res->bloom_offset      = bloom_offset;
  res->block_metas       = std::move(block_metas);
  res->block_cache       = block_cache;
  res->min_tranc_id      = min_tranc_id;
  res->max_tranc_id      = max_tranc_id;
  return res;
}