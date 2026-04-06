#include "../../include/core/memtable.h"
#include <spdlog/logger.h>
#include "spdlog/spdlog.h"
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <utility>
bool operator==(const MemTableIterator& lhs, const MemTableIterator& rhs) noexcept {
  if (lhs.queue_.empty() || rhs.queue_.empty()) {
    return lhs.queue_.empty() && rhs.queue_.empty();
  }
  return lhs.queue_.top().key_ == rhs.queue_.top().key_ &&
         lhs.queue_.top().value_ == rhs.queue_.top().value_ &&
         lhs.queue_.top().transaction_id_ == rhs.queue_.top().transaction_id_;
}
MemTableIterator::MemTableIterator()
    : current_value_(nullptr), list_iter_(nullptr), max_transaction_id(0) {}
MemTableIterator::MemTableIterator(std::vector<SerachIterator> iter, const uint64_t transaction_id)
    : max_transaction_id(transaction_id) {
  for (auto& it : iter) {
    queue_.push(it);
  }
  while (!top_value_legal()) {
    skip_transaction_id();
    while (!queue_.empty() && queue_.top().value_.empty()) {
      auto temp = queue_.top().key_;
      while (!queue_.empty() && queue_.top().key_ == temp) {
        queue_.pop();
      }
    }
  }
}
MemTableIterator::MemTableIterator(const SkiplistIterator& iter, const uint64_t transaction_id)
    : max_transaction_id(transaction_id) {
  list_iter_ = std::make_shared<SkiplistIterator>(iter);
}

auto MemTableIterator::operator<=>(const MemTableIterator& other) const {
  if (queue_.empty() || other.queue_.empty()) {
    return queue_.empty()
               ? (other.queue_.empty() ? std::strong_ordering::equal : std::strong_ordering::less)
               : std::strong_ordering::greater;
  }
  return queue_.top() <=> other.queue_.top();
}

MemTableIterator::valuetype MemTableIterator::operator*() const {
  return {current_value_->first, current_value_->second};
}
BaseIterator::pvaluetype MemTableIterator::operator->() const {
  update_current_key_value();
  return current_value_.get();
}
MemTableIterator& MemTableIterator::operator++() {
  if (queue_.empty()) {
    return *this;
  }
  auto temp = queue_.top().key_;
  queue_.pop();
  while (!queue_.empty() && queue_.top().key_ == temp) {
    queue_.pop();
  }
  while (!top_value_legal()) {
    skip_transaction_id();
    while (!queue_.empty() && queue_.top().value_.empty()) {
      auto temp = queue_.top().key_;
      while (!queue_.empty() && queue_.top().key_ == temp) {
        queue_.pop();
      }
    }
  }
  return *this;
}
bool MemTableIterator::isEnd() const {
  return queue_.empty();
}
uint64_t MemTableIterator::get_tranc_id() const {
  if (queue_.empty()) {
    return 0;
  }
  return queue_.top().transaction_id_;
}
IteratorType MemTableIterator::type() const {
  return IteratorType::MemTableIterator;
}
MemTableIterator::valuetype MemTableIterator::getValue() const {
  if (queue_.empty()) {
    return std::make_pair(std::string(), std::string());
  }
  return std::make_pair(queue_.top().key_, queue_.top().value_);
}
void MemTableIterator::pop_value() {
  if (queue_.empty()) {
    return;
  }
  auto temp = queue_.top().key_;
  while (!queue_.empty() && queue_.top().key_ == temp) {
    queue_.pop();
  }
  while (!top_value_legal()) {
    skip_transaction_id();
    while (!queue_.empty() && queue_.top().value_.empty()) {
      auto temp = queue_.top().key_;
      while (!queue_.empty() && queue_.top().key_ == temp) {
        queue_.pop();
      }
    }
  }
}
void MemTableIterator::update_current_key_value() const {
  if (!queue_.empty()) {
    current_value_ = std::make_shared<valuetype>(queue_.top().key_, queue_.top().value_);
  } else {
    current_value_.reset();
  }
}
void MemTableIterator::skip_transaction_id() {
  while (!queue_.empty() && queue_.top().transaction_id_ > max_transaction_id) {
    queue_.pop();
  }
}
bool MemTableIterator::top_value_legal() const {
  if (queue_.empty()) {
    return true;
  }
  if (max_transaction_id == 0) {
    return !queue_.top().value_.empty();
  }
  if (queue_.top().transaction_id_ <= max_transaction_id) {
    return !queue_.top().value_.empty();
  } else {
    return false;
  }
}

MemTable::MemTable()
    : current_table(std::move(std::make_unique<Skiplist>())),
      fixed_bytes(0),
      cur_status(Global_::SkiplistStatus::kNormal) {}

bool MemTableIterator::valid() const {
  return !queue_.empty();
}
std::vector<std::tuple<std::string, std::string, uint64_t>> MemTable::get_prefix_range(
    std::string_view prefix, uint64_t tranc_id) {
 std::shared_lock<std::shared_mutex> lock(cur_lock_); 
  return current_table->get_prefix_range(prefix, tranc_id);
}
void MemTable::clear() {
  auto temp     = std::move(current_table);
  current_table = std::move(std::make_unique<Skiplist>());
  fixed_tables.clear();
  fixed_bytes = 0;
}
void MemTable::put(const std::string& key, const std::string& value,
                   const uint64_t transaction_id) {
  current_table->Insert(key, value, transaction_id);
}

void MemTable::put_mutex(const std::string& key, const std::string& value,
                         const uint64_t transaction_id) {
    bool need_freeze = false;
  {
    std::unique_lock<std::shared_mutex> lock(cur_lock_);
    current_table->Insert(key, value, transaction_id);
  if (current_table->get_size() > Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    need_freeze = true;
  }
}
if (need_freeze) {
    frozen_cur_table();  // 内部自己拿 cur_lock_，多线程同时进来也安全
  }
}
void MemTable::put_batch(const std::vector<std::pair<std::string, std::string>>& key_value_pairs,
                         const uint64_t                                          transaction_id) {
  for (const auto& pair : key_value_pairs) {
    current_table->Insert(pair.first, pair.second, transaction_id);
  }
  if (current_table->get_size() > Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    std::unique_lock<std::shared_mutex> lock(fix_lock_);
    frozen_cur_table();
  }
}
std::optional<std::pair<std::string, uint64_t>> MemTable::get(std::string_view key,
                                                              const uint64_t     transaction_id) {
  std::shared_lock<std::shared_mutex> lock(cur_lock_);
  auto                                result = current_table->Get(key, transaction_id);
  if (result) {
    return std::make_pair(std::string(result->value), result->transaction_id);
  }

  lock.unlock();
  std::shared_lock<std::shared_mutex> second_lock(fix_lock_);
  for (const auto& fixed_table : fixed_tables) {
    auto result = fixed_table->Get(key, transaction_id);
    if (result) {
      return std::make_pair(std::string(result->value), result->transaction_id);
    }
  }
  return std::nullopt;
}

SkiplistIterator MemTable::cur_get(std::string_view key, const uint64_t transaction_id) {
  std::shared_lock<std::shared_mutex> lock(cur_lock_);
  return SkiplistIterator(current_table->get_node(key, transaction_id));
}
SkiplistIterator MemTable::fix_get(std::string_view key, const uint64_t transaction_id) {
  std::shared_lock<std::shared_mutex> lock(fix_lock_);
  for (const auto& result : fixed_tables) {
    if (result->Contain(key, transaction_id).has_value()) {
      return SkiplistIterator(result->get_node(key, transaction_id));
    }
  }
  return SkiplistIterator();
}

std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>> MemTable::get_batch(
    const std::vector<std::string>& key_pairs, const uint64_t transaction_id) {
  std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>> result;
  result.reserve(key_pairs.size());
  for (const auto& pair : key_pairs) {
    auto value = get(pair, transaction_id);
    if (value.has_value()) {  // 添加空值检查
      result.emplace_back(pair, value->first, value->second);
    } else {
      result.emplace_back(pair, std::nullopt, transaction_id);
    }
  }
  return result;
}
std::size_t MemTable::get_node_num() const {
  std::size_t result = 0;
  for (auto& it : fixed_tables) {
    result += it->getnodecount();
  }
  return result + current_table->getnodecount();
}
std::size_t MemTable::get_fixed_size() {
  return fixed_bytes;
}
std::size_t MemTable::get_cur_size() {
  return current_table->get_size();
}
std::size_t MemTable::get_total_size() {
  return get_cur_size() + get_fixed_size();
}

void MemTable::remove(std::string_view key, const uint64_t transaction_id) {
  current_table->Insert(std::string(key), std::string(), transaction_id);
  if (current_table->get_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    frozen_cur_table();
  }
}
void MemTable::remove_mutex(std::string_view key, const uint64_t transaction_id) {
  {
    std::unique_lock<std::shared_mutex> lock(cur_lock_);
    current_table->Insert(std::string(key), std::string(), transaction_id);
  }
  if (fixed_tables.size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    frozen_cur_table();
  }
}
// 批量删除
void MemTable::remove_batch(const std::vector<std::string>& key_pairs,
                            const uint64_t                  transaction_id) {
  std::unique_lock<std::shared_mutex> lock(cur_lock_);
  for (const auto& pair : key_pairs) {
    current_table->Insert(pair, std::string(), transaction_id);
  }
  if (current_table->get_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    frozen_cur_table();
  }
}
bool MemTable::IsFull() {
  return current_table->get_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE;
}
std::unique_ptr<Skiplist> MemTable::flushtodisk() {
  std::unique_lock<std::shared_mutex> lock(fix_lock_);
  if (fixed_tables.empty()) {
  return nullptr;
  }
  auto                                temp = std::move(fixed_tables.front());
  fixed_tables.pop_front();
  fixed_bytes -= temp->get_size();
  return temp;
}

// This function is used to flush the current memtable to disk,just for test
std::unique_ptr<Skiplist> MemTable::flush() {
  std::unique_lock<std::shared_mutex> lock(cur_lock_);
  auto                                new_table = std::move(std::make_unique<Skiplist>());
  auto                                temp      = std::move(current_table);
  current_table                                 = std::move(new_table);
  fixed_bytes += current_table->get_size();
  return temp;
}
std::list<std::unique_ptr<Skiplist>> MemTable::flushsync() {
  std::unique_lock<std::shared_mutex> lock(cur_lock_);
  auto                                new_table = std::make_unique<Skiplist>();
  fixed_bytes += current_table->get_size();
  fixed_tables.emplace_back(std::move(current_table));
  current_table = std::move(new_table);
  return std::move(fixed_tables);
}
bool MemTable::frozen_cur_table(bool force) {
  std::unique_lock<std::shared_mutex> lock(cur_lock_);
  
  // 空表不冻结，无论是否 force
  if (current_table->get_size() == 0) {
    return false;
  }
  
  if (!force && current_table->get_size() < Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    return false;
  }
  
  auto new_table = std::make_unique<Skiplist>();
  fixed_bytes += current_table->get_size();
  current_table->set_status(Global_::SkiplistStatus::kFrozen);
  {
    std::unique_lock<std::shared_mutex> lock2(fix_lock_);
    fixed_tables.push_back(std::move(current_table));
  }

  current_table = std::move(new_table);
  return true;
}

MemTableIterator MemTable::begin() {
  return MemTableIterator(fixed_tables.begin()->get()->begin(), 0);
}
MemTableIterator MemTable::end() {
  return MemTableIterator(fixed_tables.end()->get()->end(), 0);
}
// 迭代器

MemTableIterator MemTable::prefix_serach(std::string_view key, const uint64_t transaction_id) {
  std::vector<SerachIterator>         iter;
  std::shared_lock<std::shared_mutex> lock(cur_lock_);
  if (!current_table) {
    spdlog::info("current_table is null");
    return MemTableIterator(iter, transaction_id);
  }
  auto begin_iter = current_table->prefix_serach_begin(key);
  auto end_iter   = current_table->prefix_serach_end(key);
  if (!begin_iter.valid()) {
    return MemTableIterator(iter, transaction_id);
  }
  for (auto begin = begin_iter; begin != end_iter; ++begin) {
    iter.push_back(
        SerachIterator(begin.getValue().first, begin.getValue().second, transaction_id, 0));
  }
  lock.unlock();
  std::shared_lock<std::shared_mutex> second_lock(fix_lock_);
  if (fixed_tables.empty()) {
    return MemTableIterator(iter, transaction_id);
  }
  for (const auto& fixed_table : fixed_tables) {
    for (auto begin = fixed_table->prefix_serach_begin(key);
         begin != fixed_table->prefix_serach_end(key); ++begin) {
      iter.push_back(
          SerachIterator(begin.getValue().first, begin.getValue().second, transaction_id, 0));
    }
  }
  return MemTableIterator(iter, transaction_id);
}
