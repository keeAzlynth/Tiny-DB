#pragma once
#include "BaseIterator.h"
#include <atomic>
#include <concepts>
#include <ranges>
#include <memory>
#include <array>
#include <optional>
#include <random>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>
#include "../include/Global.h"

class SkiplistIterator;
class Node {
 public:
  std::string                           key_;
  std::string                           value_;
  std::unique_ptr<Node>                 next_;
  std::array<Node*, Global_::FIX_LEVEL> forward;
  uint64_t                              transaction_id;
  Node(const std::string& key, const std::string& value, const uint64_t transaction_ids = 0)
      : key_(key), value_(value), next_{}, transaction_id(transaction_ids) {
    forward.fill(nullptr);
  }
  auto operator<=>(const Node& other) const;
};

bool operator==(const SkiplistIterator& lhs, const SkiplistIterator& rhs) noexcept;
class SkiplistIterator : public BaseIterator {
  friend class Skiplist;  // 让 Skiplist 可以访问私有成员
  friend bool operator==(const SkiplistIterator& lhs, const SkiplistIterator& rhs) noexcept;

 public:
  using valuetype = std::pair<std::string, std::string>;
  SkiplistIterator(Node* skiplist);
  SkiplistIterator();
  ~SkiplistIterator() = default;
  BaseIterator& operator++() override;
  auto          operator<=>(const SkiplistIterator& other) const;

  valuetype                           operator*() const override;
  SkiplistIterator                    operator+=(int offset) const;
  bool                                valid() const override;
  bool                                isEnd() const override;
  IteratorType                        type() const override;
  uint64_t                            get_tranc_id() const override;
  std::pair<std::string, std::string> getValue() const;

 private:
  Node* current;
};

inline int cmp(const std::string& a, const std::string& b) {
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}
class Skiplist {
 public:
  explicit Skiplist(int max_level_ = Global_::MAX_LEVEL)
      : max_level(max_level_), current_level(1), size_bytes(0), nodecount(0), dis(0.0, 1.0) {
    head = std::make_unique<Node>(std::string(), std::string(), 0);
    size_bytes += sizeof(uint64_t) + 8 * sizeof(Global_::FIX_LEVEL + 1);
  }  // 默认最大取决FIX_LEVEL层
  Skiplist(const Skiplist& other)            = delete;
  Skiplist& operator=(const Skiplist& other) = delete;
  Skiplist(Skiplist&& other) noexcept
      : max_level(other.max_level),
        head(std::move(other.head)),
        current_level(other.current_level),
        size_bytes(other.size_bytes.load()),
        nodecount(other.nodecount.load()) {
    other.head = nullptr;
    size_bytes.exchange(0, std::memory_order_relaxed);
    nodecount.exchange(0, std::memory_order_relaxed);
  }
  Skiplist& operator=(Skiplist&& other) noexcept {
    std::ranges::swap(max_level, other.max_level);
    std::ranges::swap(head, other.head);
    std::ranges::swap(current_level, other.current_level);
    size_bytes.exchange(other.size_bytes, std::memory_order_relaxed);
    nodecount.exchange(other.nodecount, std::memory_order_relaxed);
    return *this;
  }

  bool Insert(const std::string& key, const std::string& value, const uint64_t transaction_id = 0);

  bool Delete(const std::string& key);

  std::optional<std::string> Contain(const std::string& key, const uint64_t transaction_id = 0);
  std::unique_ptr<Node>      Get(const std::string& key, const uint64_t transaction_id = 0);
  std::vector<std::pair<std::string, std::string>> flush();
  Node*            get_node(const std::string& key, const uint64_t transaction_id = 0);
  std::size_t      get_size();
  std::size_t      getnodecount();
  int              get_range_index(const std::string& key);
  auto             seekToFirst();
  auto             seekToLast();
  SkiplistIterator end();
  SkiplistIterator begin();
  SkiplistIterator prefix_serach_begin(const std::string& key);
  SkiplistIterator prefix_serach_end(const std::string& key);

  void                    set_status(Global_::SkiplistStatus status);
  Global_::SkiplistStatus get_status() const;

 private:
  std::unique_ptr<Node>            head;
  int                              max_level;      // 最大层级
  int                              current_level;  // 当前层级
  std::atomic_size_t               size_bytes;     // 内存占用，达到限制flush到disk
  std::atomic_int                  nodecount;  // 节点数量
  std::random_device               rd;             // 随机数生成器
  static thread_local std::mt19937 gen;            // 随机数引擎
  std::uniform_real_distribution<> dis;            // 随机数分布

  Global_::SkiplistStatus cur_status = Global_::SkiplistStatus::kNormal;
  int                     random_level();
};
