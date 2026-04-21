#include "../../include/core/Skiplist.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <string_view>

auto Node::operator<=>(const Node& other) const {
  return key_ <=> other.key_;
}

SkiplistIterator::SkiplistIterator(Node* skiplist) {
  current = skiplist;
}
SkiplistIterator::SkiplistIterator() : current(nullptr) {}
BaseIterator& SkiplistIterator::operator++() {
  if (current) {
    current = current->next_.get();
  }
  return *this;
}
auto SkiplistIterator::operator<=>(const SkiplistIterator& other) const {
  return current <=> other.current;
}
bool operator==(const SkiplistIterator& lhs, const SkiplistIterator& rhs) noexcept {
  return lhs.current == rhs.current;
}

SkiplistIterator::valuetype SkiplistIterator::operator*() const {
  if (current) {
    return {current->key_, current->value_};
  }
  return {};
}
SkiplistIterator SkiplistIterator::operator+=(int offset) const {
  SkiplistIterator result = *this;
  for (int i = 0; i < offset && result.valid(); ++i) {
    ++result;
  }
  return result;
}
bool SkiplistIterator::valid() const {
  return current != nullptr;
}
bool SkiplistIterator::isEnd() const {
  return current == nullptr;
}
IteratorType SkiplistIterator::type() const {
  return IteratorType::SkiplistIterator;
}
uint64_t SkiplistIterator::get_tranc_id() const {
  if (current) {
    return current->transaction_id;
  }
  return 0;
}
SkiplistIterator::valuetype SkiplistIterator::getValue() const {
  if (current) {
    return {current->key_, current->value_};
  }
  return {};
}
std::tuple<std::string, std::string, uint64_t> SkiplistIterator::get_value_tranc_id() const {
  if (current) {
    return {current->key_, current->value_, current->transaction_id};
  }
  return {std::string(), std::string(), 0};
}

bool Skiplist::Insert(std::string key, std::string value, const uint64_t transaction_id) {
  std::array<Node*, Global_::FIX_LEVEL> update;
  update.fill(nullptr);
  auto current = head.get();

  // 查找插入位置
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward[i] && cmp(current->forward[i]->key_, key) == -1) {
      current = current->forward[i];
    }
    update[i] = current;  // 记录需要更新的节点
  }
  // 拿到新的节点的高度
  int Newlevel = random_level();
  if (Newlevel > current_level) {
    for (int i = current_level; i < Newlevel; i++) {
      update[i] = head.get();
    }
    current_level = Newlevel;  // 提前更新 current_level
  }
 
  // 创建新节点
  auto NewNode = std::make_unique<Node>(key, value, transaction_id);
  // 插入新节点 
  size_bytes += (key.size() + value.size() + sizeof(uint64_t) + 8 * (Global_::FIX_LEVEL + 1));
  for (int i = 0; i < Newlevel; i++) {
    NewNode->forward[i]   = update[i]->forward[i];
    update[i]->forward[i] = NewNode.get();
  }
  NewNode->next_   = std::move(update[0]->next_);
  update[0]->next_ = std::move(NewNode);
  // 更新当前层级
  current_level = std::max(current_level, Newlevel);
  nodecount++;
  return true;
}

bool Skiplist::Delete(std::string_view key) {
  auto                                  current = head.get();
  std::array<Node*, Global_::FIX_LEVEL> update{};

  // 查找删除位置
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && current->forward[i]->key_ < key) {
      current = current->forward[i];
    }
    update[i] = current;  // 记录需要更新的节点
  }
  // 删除节点
  auto target = current->forward[0];
  if (target && target->key_ == key) {
    for (int i = 0; i < current_level; ++i) {
      if (update[i]) {
        update[i]->forward[i] = target->forward[i];
      }
    }
    size_bytes -= (target->key_.size() + target->value_.size() + sizeof(uint64_t) +
                   8 * (Global_::FIX_LEVEL + 1));
    update[0]->next_ = std::move(target->next_);
    nodecount--;
  }

  // 更新当前层级
  // 如果当前层级的节点为空，则需要更新当前层级
  for (int i = current_level - 1; i >= 0; --i) {
    if (head->forward[i] == nullptr && nodecount) {
      current_level--;
    }
  }
  return true;
}
  void Skiplist::set_num_shard(int num_shard){
    num_shard_=num_shard;
  }
  int Skiplist::get_num_shard()const{
return num_shard_;
  }
std::optional<std::string> Skiplist::Contain(std::string_view key, const uint64_t transaction_id) {
  auto current = head.get();
  // 从最高层开始查找
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward[i] && cmp(current->forward[i]->key_, key) == -1) {
      current = current->forward[i];
    }
  }
  if (current->forward[0] && cmp(current->forward[0]->key_, key) == 0) {
    if (transaction_id == 0) {
      return current->forward[0]->value_;
    } else {
      while (current->forward[0] && cmp(current->forward[0]->key_, key) == 0 &&
             current->forward[0]->transaction_id > transaction_id) {
        current = current->forward[0];
      }
      if (current->forward[0] && cmp(current->forward[0]->key_, key) == 0) {
        return current->forward[0]->value_;
      } else if (cmp(current->key_, key) == 0 && current->transaction_id <= transaction_id) {
        return current->value_;
      }
      return std::nullopt;
    }
  }
  return std::nullopt;  // 如果没有找到，返回空值
}

std::optional<LookupResult> Skiplist::Get(std::string_view key, const uint64_t transaction_id) {
  auto current = head.get();
  // 从最高层开始查找
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward[i] && cmp(current->forward[i]->key_, key) == -1) {
      current = current->forward[i];
    }
  }
  if (current->forward[0] && cmp(current->forward[0]->key_, key) == 0) {
    if (transaction_id == 0) {
      current = current->forward[0];
      return LookupResult(current->value_, current->transaction_id);
    } else {
      while (current->forward[0] && cmp(current->forward[0]->key_, key) == 0 &&
             current->forward[0]->transaction_id > transaction_id) {
        current = current->forward[0];
      }
      if (current->forward[0] && cmp(current->forward[0]->key_, key) == 0) {
        current = current->forward[0];
        return LookupResult(current->value_, current->transaction_id);
      } else if (cmp(current->key_, key) == 0 && current->transaction_id <= transaction_id) {
        return LookupResult(current->value_, current->transaction_id);
      }
      return std::nullopt;
    }
  }
  return std::nullopt;  // 如果没有找到，返回空值
}

std::vector<std::pair<std::string, std::string>> Skiplist::flush() {
  std::vector<std::pair<std::string, std::string>> result;
  auto                                             current = head->forward[0];
  while (current) {
    result.emplace_back(current->key_, current->value_);
    current = current->forward[0];
  }
  return result;
}
Node* Skiplist::get_node(std::string_view key, const uint64_t transaction_id) {
  auto current = head.get();
  // 从最高层开始查找
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward[i] && cmp(current->forward[i]->key_, key) == -1) {
      current = current->forward[i];
    }
  }
  if (current->forward[0] && cmp(current->forward[0]->key_, key) == 0) {
    if (transaction_id == 0) {
      current = current->forward[0];
      return current;
    } else {
      while (current->forward[0] && cmp(current->forward[0]->key_, key) == 0 &&
             current->forward[0]->transaction_id > transaction_id) {
        current = current->forward[0];
      }
      if (current->forward[0] && cmp(current->forward[0]->key_, key) == 0 &&
          (!current->forward[0]->value_.empty())) {
        current = current->forward[0];
        return current;
      } else if (cmp(current->key_, key) == 0 && current->transaction_id <= transaction_id &&
                 (!current->value_.empty())) {
        return current;
      }
      return nullptr;
    }
  }
  return nullptr;  // 如果没有找到，返回空值
}

std::size_t Skiplist::get_size() {
  return size_bytes;
}

std::size_t Skiplist::getnodecount() {
  return nodecount;
}

auto Skiplist::seekToFirst() {
  return head->forward[0];
}  // 定位到第一个元素
auto Skiplist::seekToLast() {
  auto current = head->forward[0];
  while (current->forward[0]) {
    current = current->forward[0];
  }
  return current;
}
SkiplistIterator Skiplist::end() {
  return SkiplistIterator(nullptr);
}
SkiplistIterator Skiplist::begin() {
  return SkiplistIterator(head->forward[0]);
}

SkiplistIterator Skiplist::prefix_serach_begin(std::string_view key) {
  auto current = head.get();
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && cmp(current->forward[i]->key_, key) == -1) {
      current = current->forward[i];
    }
  }
  if (current && current->key_.starts_with(key)) {
    return SkiplistIterator(current);
  }
  if (current->forward[0] && current->forward[0]->key_.starts_with(key)) {
    return SkiplistIterator(current->forward[0]);
  }
  return SkiplistIterator(nullptr);
}
SkiplistIterator Skiplist::prefix_serach_end(std::string_view key) {
  std::string Newkey{key};
  Newkey += '\xFF';
  auto current = head.get();
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && cmp(current->forward[i]->key_, Newkey) == -1) {
      current = current->forward[i];
    }
  }
  return SkiplistIterator(current->forward[0]);
}
std::vector<std::tuple<std::string, std::string, uint64_t>> Skiplist::get_prefix_range(
    std::string_view prefix, uint64_t tranc_id) {
  auto                                                        end = prefix_serach_end(prefix);
  std::vector<std::tuple<std::string, std::string, uint64_t>> result;
  for (auto begin = prefix_serach_begin(prefix); begin != end; ++begin) {
    result.emplace_back(begin.get_value_tranc_id());
  }
  return result;
}
void Skiplist::set_status(Global_::SkiplistStatus status) {
  cur_status = status;
}

Global_::SkiplistStatus Skiplist::get_status() const {
  return cur_status;
}

thread_local std::mt19937 Skiplist::gen(std::random_device{}());
int                       Skiplist::random_level() {
  static constexpr double P     = 0.25;  // 每一层的概率
  int                     level = 1;
  while (dis(gen) < P && level < Global_::FIX_LEVEL) {
    ++level;
  }
  return level;
}