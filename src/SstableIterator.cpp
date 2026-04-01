#include "spdlog/spdlog.h"

#include "../include/Sstable.h"
#include "../include/SstableIterator.h"
#include <optional>
#include <print>
#include <string>
#include <tuple>

std::optional<std::pair<SstIterator, SstIterator>> SstIterator::find_prefix_key(
    std::shared_ptr<Sstable> sstable, std::string prefix, uint64_t tranc_id) {
  auto res1 = sstable->find_block_range(prefix);
  if (res1.empty()) {
    return std::make_pair<SstIterator>(SstIterator(), SstIterator());
  }
  auto begin = std::make_shared<BlockIterator>(BlockIterator(res1[0], prefix, tranc_id));
  auto end =
      std::make_shared<BlockIterator>(BlockIterator(res1[res1.size() - 1], prefix, tranc_id));
  return std::make_pair<SstIterator>(SstIterator(sstable, begin, prefix, tranc_id),
                                     SstIterator(sstable, end, prefix, tranc_id));
}
bool operator==(const SstIterator& rhs, const SstIterator& lhs) noexcept {
  return rhs.m_sst == lhs.m_sst && rhs.m_block_idx == lhs.m_block_idx &&
         rhs.m_block_it == lhs.m_block_it && rhs.max_tranc_id_ == lhs.max_tranc_id_;
}
SstIterator::SstIterator() {
  m_sst         = nullptr;
  m_block_it    = nullptr;
  cached_value  = {std::string(), std::string()};
  m_block_idx   = 0;
  max_tranc_id_ = 0;
}
SstIterator::SstIterator(std::shared_ptr<Sstable> sst, uint64_t tranc_id)
    : m_sst(sst), max_tranc_id_(tranc_id) {
  if (!m_sst || m_sst->num_blocks() == 0) {
    m_block_it = nullptr;
    return;
  }
  m_block_idx = 0;
  auto block  = m_sst->read_block(m_block_idx);
  m_block_it  = std::make_shared<BlockIterator>(block, 0, tranc_id);
}

SstIterator::SstIterator(std::shared_ptr<Sstable> sst, const std::string& key, uint64_t tranc_id,
                         bool is_prefix)
    : m_sst(sst), max_tranc_id_(tranc_id) {
  seek(key, is_prefix);
}
SstIterator::SstIterator(std::shared_ptr<Sstable> sst, size_t block_idx,
                         const std::string& key = std::string(), uint64_t tranc_id = 0)
    : m_sst(sst), m_block_idx(block_idx), max_tranc_id_(tranc_id) {
  if (!m_sst || !is_block_index_vaild(block_idx)) {
    m_block_it = nullptr;
    return;
  }
  auto block = m_sst->read_block(m_block_idx);
  if (exists_key_prefix(key)) {
    m_block_it = std::make_shared<BlockIterator>(block, key, tranc_id);
    return;
  }
  m_block_it = std::make_shared<BlockIterator>(block, 0, tranc_id);
}

SstIterator::SstIterator(std::shared_ptr<Sstable> sst, std::shared_ptr<BlockIterator> block_iter,
                         std::string key, uint64_t tranc_id)
    : m_sst(sst),
      m_block_it(block_iter),
      m_block_idx(m_block_it->getIndex()),
      max_tranc_id_(tranc_id) {}
void SstIterator::set_end() {
  m_block_it     = nullptr;
  auto meta_size = get_Block_Meta_size();
  if (meta_size.has_value()) {
    m_block_idx = meta_size.value();
    return;
  }
  m_block_idx = -1;
}
void SstIterator::seek(const std::string& key, bool is_prefix) {
  if (!m_sst) {
    set_end();
    return;
  }

  auto find_result = m_sst->find_block_idx(key, is_prefix);
  if (!find_result.has_value()) {
    set_end();
    return;
  }
  m_block_idx = find_result.value();

  auto block = m_sst->read_block(m_block_idx);
  m_block_it = std::make_shared<BlockIterator>(block, key, max_tranc_id_, is_prefix);
  if (m_block_it->is_end()) {
    set_end();
    return;
  }
  update_current();
}
std::string SstIterator::key() const {
  if (!m_block_it) {
    spdlog::info("SstIterator::key() BlockIterator is invalid");
    return std::string();
  }
  return (*m_block_it)->first;
}

std::string SstIterator::value() const {
  if (!m_block_it) {
    spdlog::info("SstIterator::value() BlockIterator is invalid");
    return std::string();
  }
  return (*m_block_it)->second;
}

std::tuple<std::string, std::string, uint64_t> SstIterator::getValue() const {
  if (m_block_it) {
    return std::make_tuple(key(), value(), m_block_it->get_cur_tranc_id());
  }
  return std::make_tuple(std::string(), std::string(), 0);
}

BaseIterator& SstIterator::operator++() {
  if (!m_block_it) {  // 添加空指针检查
    return *this;
  }
  ++(*m_block_it);
  if (m_block_it->is_end()) {
    m_block_idx++;
    if (is_block_index_vaild(m_block_idx)) {
      // 读取下一个block
      auto          next_block = m_sst->read_block(m_block_idx);
      BlockIterator new_block_it(next_block, 0, max_tranc_id_);
      cached_value  = std::nullopt;
      (*m_block_it) = new_block_it;
    } else {
      // 没有下一个block
      m_block_it = nullptr;
    }
  }
  return *this;
}
bool SstIterator::isEnd() const {
  return m_block_it && m_sst->num_blocks() <= m_block_idx;
}
bool SstIterator::is_block_index_vaild(size_t block_index) const {
  return m_sst->is_block_index_vaild(block_index);
}

bool SstIterator::exists_key_prefix(std::string key) const {
  if (!m_block_it) {
    return false;
  }
  if (m_block_it->get_block()->get_prefix_begin_offset_binary(key).has_value()) {
    return true;
  }
  return false;
}

bool SstIterator::valid() const {
  return m_block_it && !m_block_it->is_end();
}

auto SstIterator::operator<=>(const SstIterator& rhs) const -> std::strong_ordering {
  if (m_block_idx != rhs.m_block_idx) {
    return m_block_idx <=> rhs.m_block_idx;
  }
  if (m_block_it && rhs.m_block_it) {
    return (*m_block_it) <=> (*rhs.m_block_it);
  }
  return std::strong_ordering::equal;
}

SstIterator::valuetype SstIterator::operator*() const {
  if (!m_block_it) {
    spdlog::info("SstIterator::operator*() Iterator is invalid");
    return {std::string(), std::string()};
  }
  return (**m_block_it);
}

uint64_t SstIterator::get_tranc_id() const {
  return max_tranc_id_;
}
std::optional<size_t> SstIterator::get_Block_Meta_size() const {
  if (m_sst) {
    return m_sst->num_blocks();
  }
  return std::nullopt;
}
IteratorType SstIterator::type() const {
  return IteratorType::SstIterator;
}

BlockIterator::con_pointer SstIterator::operator->() const {
  if (cached_value.has_value()) {
    return &(*cached_value);
  }
  return nullptr;
}
size_t SstIterator::get_block_idx() const {
  return m_block_idx;
}

std::shared_ptr<Sstable> SstIterator::get_sstable() const {
  return m_sst;
}

MemTableIterator SstIterator::merge_sst_iterator(std::vector<SstIterator> iter_vec,
                                                 uint64_t                 tranc_id) {
  if (iter_vec.empty()) {
    return MemTableIterator();
  }

  MemTableIterator it_begin{};
  for (auto& iter : iter_vec) {
    while (iter.valid()) {
      it_begin.queue_.emplace(iter.key(), iter.value(), iter.get_tranc_id(),
                              iter.m_sst->get_sst_id(), 0);
      ++iter;
    }
  }
  return it_begin;
}
void SstIterator::update_current() const {
  if (valid()) {
    cached_value = **m_block_it;
  }
}