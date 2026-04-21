#include "../../include/core/memtable.h"
#include <spdlog/logger.h>
#include "spdlog/spdlog.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
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
    :fixed_bytes(0), cur_status(Global_::SkiplistStatus::kNormal) {
  for (auto it=0;it<current_table.size();it++) {
    current_table[it] = std::move(std::make_unique<Skiplist>());
    current_table[it]->set_num_shard(it);
  }
}

bool MemTableIterator::valid() const {
  return !queue_.empty();
}
std::vector<std::tuple<std::string, std::string, uint64_t>> MemTable::get_prefix_range(
    std::string_view prefix, uint64_t tranc_id) {
std::vector<std::tuple<std::string, std::string, uint64_t>> res;
 for (auto index=0; index<current_table.size();index++) {
  std::shared_lock<std::shared_mutex> lock(cur_lock_[index]);
  auto res1= current_table[index]->get_prefix_range(prefix, tranc_id);
  if (!res1.empty()) {
  std::ranges::move(res1,std::back_inserter(res));
  }
 }
       std::shared_lock<std::shared_mutex> lock_fix(fix_lock_);
  for (auto &it:fixed_tables) {
  auto res2=it->get_prefix_range(prefix, tranc_id);
  if (!res2.empty()) {
  std::ranges::move(res2,std::back_inserter(res));
  }
  }
  return res;
}
void MemTable::clear() {
  // Sharding mode: clear all shards
  for (auto& table : current_table) {
    table = std::move(std::make_unique<Skiplist>());
  }
  fixed_tables.clear();
  fixed_bytes = 0;
}
void MemTable::put(const std::string& key, const std::string& value, const uint64_t transaction_id,
                   const size_t shard_idx) {
    current_table[shard_idx]->Insert(key, value, transaction_id);
}

void MemTable::put_mutex(const std::string& key, const std::string& value,
                         const uint64_t transaction_id) {
  auto index = Global_::fast_hash(key);
   bool need_freeze = false;
    {
      std::unique_lock<std::shared_mutex> lock(cur_lock_[index]);
      current_table[index]->Insert(key, value, transaction_id);
      if (current_table[index]->get_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
        need_freeze = true;
      }
    }
    if (need_freeze) {
      frozen_cur_table(false,index);  
    }
  return;
}
void MemTable::put_batch(const std::vector<std::pair<std::string, std::string>>& key_value_pairs,
                         const uint64_t                                          transaction_id) {

    // Sharding mode: distribute to appropriate shards
    for (const auto& pair : key_value_pairs) {
      auto index = Global_::fast_hash(pair.first);
      std::unique_lock<std::shared_mutex> lock(cur_lock_[index]);
      current_table[index]->Insert(pair.first, pair.second, transaction_id);
  }
}
std::optional<std::pair<std::string, uint64_t>> MemTable::get(std::string_view key,
                                                              const uint64_t   transaction_id) {
      auto index = Global_::fast_hash(key);
std::shared_lock<std::shared_mutex> lock(cur_lock_[index]);
    auto                                result = current_table[index]->Get(key, transaction_id);
    if (result.has_value()) {
      return std::make_pair(std::move(result->value), result->transaction_id);
    }
lock.unlock();
  // Check fixed tables
  std::shared_lock<std::shared_mutex> second_lock(fix_lock_);
  for (const auto& fixed_table : fixed_tables) {
    if(fixed_table->get_num_shard()==index){
      auto res=fixed_table->Get(key);
      if (res.has_value()) {
      return std::make_pair(std::move(res->value), res->transaction_id);
    } 
    }
  }
  return std::nullopt;
}
SkiplistIterator MemTable::cur_get(std::string_view key, const uint64_t transaction_id) {
  std::shared_lock<std::shared_mutex> lock(cur_lock_[0]);
  return SkiplistIterator(current_table[0]->get_node(key, transaction_id));
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
  for (auto& it : current_table) {
    result += it->getnodecount();
  }
  for (auto& it : fixed_tables) {
    result += it->getnodecount();
  }
  return result;
}
std::size_t MemTable::get_fixed_size() {
  return fixed_bytes;
}
std::size_t MemTable::get_cur_size() {
  std::size_t result = 0;
  for (auto& it : current_table) {
    result += it->get_size();
  }
  return result;
}
std::size_t MemTable::get_total_size() {
  return get_cur_size() + get_fixed_size();
}

void MemTable::remove(std::string key, const uint64_t transaction_id) {
put(key, std::string(), transaction_id);
}
void MemTable::remove_mutex(std::string key, const uint64_t transaction_id) {
  put_mutex(key, std::string(), transaction_id);
}
// 批量删除
void MemTable::remove_batch(const std::vector<std::string>& key_value_pairs,
                            const uint64_t                  transaction_id) {
    // Sharding mode: distribute to appropriate shards
    for (const auto& pair : key_value_pairs) {
      auto index = std::hash<std::string_view>{}(pair) % Global_::NUMS_SHARDS;
      std::unique_lock<std::shared_mutex> lock(cur_lock_[index]);
      current_table[index]->Insert(pair, std::string(), transaction_id);
    }
}
bool MemTable::IsFull(size_t target) {
  return current_table[target]->get_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE;
}
std::unique_ptr<Skiplist> MemTable::flushtodisk() {
  std::unique_lock<std::shared_mutex> lock(fix_lock_);
  if (fixed_tables.empty()) {
    return nullptr;
  }
  auto temp = std::move(fixed_tables.front());
  fixed_tables.pop_front();
  fixed_bytes -= temp->get_size();
  return temp;
}

// This function is used to flush the current memtable to disk,just for test
std::list<std::unique_ptr<Skiplist>> MemTable::flush() {
std::list<std::unique_ptr<Skiplist>> res;
for (auto &it:current_table) {
if (it->getnodecount()==0) {
continue;
}
res.emplace_back(std::move(it));
}
return res;
}
std::list<std::unique_ptr<Skiplist>> MemTable::flushsync() {
  std::unique_lock<std::shared_mutex> lock(cur_lock_[0]);
  auto                                new_table = std::make_unique<Skiplist>();
  fixed_bytes += current_table[0]->get_size();
  fixed_tables.emplace_back(std::move(current_table[0]));
  current_table[0] = std::move(new_table);
  return std::move(fixed_tables);
}
bool MemTable::frozen_cur_table(bool force, size_t target) {
  if (!force) {
    // ── non-force: freeze only the target shard ───────────────────────────
    std::unique_lock<std::shared_mutex> lock(cur_lock_[target]);
    if (current_table[target]->getnodecount()==0||current_table[target]->get_size() == 0) return false;
    if (current_table[target]->get_size() < Global_::MAX_MEMTABLE_SIZE_PER_TABLE)
      return false;

    auto new_table = std::make_unique<Skiplist>();
    auto temp      = std::move(current_table[target]);
    new_table->set_num_shard(target);
    current_table[target] = std::move(new_table);
    lock.unlock();          // released once, correctly

    temp->set_status(Global_::SkiplistStatus::kFrozen);
    std::unique_lock<std::shared_mutex> lk2(fix_lock_);
    fixed_bytes += temp->get_size();
    fixed_tables.push_back(std::move(temp));
    return true;
  }

  // ── force: freeze every non-empty shard independently ────────────────────
  for (size_t index = 0; index < current_table.size(); ++index) {
    std::unique_ptr<Skiplist> temp;
    {
      std::unique_lock<std::shared_mutex> lock(cur_lock_[index]); // one lock per shard
      if (current_table[index]->getnodecount() == 0) continue;
      auto new_table = std::make_unique<Skiplist>();
      new_table->set_num_shard(index);
      temp = std::move(current_table[index]);
      current_table[index] = std::move(new_table);
    }  // ← lock released here, once, correctly
    temp->set_status(Global_::SkiplistStatus::kFrozen);
    std::unique_lock<std::shared_mutex> lk2(fix_lock_);
    fixed_bytes += temp->get_size();
    fixed_tables.push_back(std::move(temp));
  }
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
  std::shared_lock<std::shared_mutex> lock(cur_lock_[0]);
  if (!current_table[0]) {
    spdlog::info("current_table is null");
    return MemTableIterator(iter, transaction_id);
  }
  auto begin_iter = current_table[0]->prefix_serach_begin(key);
  auto end_iter   = current_table[0]->prefix_serach_end(key);
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
std::vector<size_t> MemTable::getShardNodeCounts() const {
  std::vector<size_t> counts;
    for (const auto& table : current_table) {
      counts.push_back(table->getnodecount());
    }
  return counts;
}
