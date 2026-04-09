#pragma once
#include "../iterator/BaseIterator.h"
#include <atomic>
#include <concepts>
#include <ranges>
#include <memory>
#include <array>
#include <optional>
#include <random>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
#include "Global.h"

class SkiplistIterator;
struct LookupResult {
  std::string_view value;
  uint64_t         transaction_id;
};
class Node {
 public:
  std::string                           key_;
  std::string                           value_;
  std::unique_ptr<Node>                 next_;
  std::array<Node*, Global_::FIX_LEVEL> forward;
  uint64_t                              transaction_id;
  Node(std::string key, std::string value, const uint64_t transaction_ids = 0)
      : key_(std::move(key)), value_(std::move(value)), next_{}, transaction_id(transaction_ids) {
    forward.fill(nullptr);
  }
  Node(Node&&) noexcept            = default;
  Node& operator=(Node&&) noexcept = default;
  Node(const Node&)                = delete;
  Node& operator=(const Node&)     = delete;
  auto  operator<=>(const Node& other) const;
};

bool operator==(const SkiplistIterator& lhs, const SkiplistIterator& rhs) noexcept;
class SkiplistIterator : public BaseIterator {
  friend class Skiplist;  // 让 Skiplist 可以访问私有成员
  friend bool operator==(const SkiplistIterator& lhs, const SkiplistIterator& rhs) noexcept;

 public:
  SkiplistIterator(Node* skiplist);
  SkiplistIterator();
  ~SkiplistIterator() = default;
  BaseIterator& operator++() override;
  auto          operator<=>(const SkiplistIterator& other) const;

  valuetype                                      operator*() const override;
  SkiplistIterator                               operator+=(int offset) const;
  bool                                           valid() const override;
  bool                                           isEnd() const override;
  IteratorType                                   type() const override;
  uint64_t                                       get_tranc_id() const override;
  valuetype                                      getValue() const;
  std::tuple<std::string, std::string, uint64_t> get_value_tranc_id() const;

 private:
  Node* current;
};

inline int cmp(std::string_view a, std::string_view b) {
  auto res = a <=> b;
  if (res < 0)
    return -1;
  if (res > 0)
    return 1;
  return 0;
}

class Skiplist {
 public:
  explicit Skiplist(int max_level_ = Global_::MAX_LEVEL)
      : max_level(max_level_), current_level(1), size_bytes(0), nodecount(0), dis(0.0, 1.0) {
    head = std::make_unique<Node>(std::string(), std::string(), 0);
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

  bool Insert(std::string key, std::string value, const uint64_t transaction_id = 0);

  bool Delete(std::string_view key);

  std::optional<std::string>  Contain(std::string_view key, const uint64_t transaction_id = 0);
  std::optional<LookupResult> Get(std::string_view key, const uint64_t transaction_id = 0);
  std::vector<std::pair<std::string, std::string>> flush();
  Node*            get_node(std::string_view key, const uint64_t transaction_id = 0);
  std::size_t      get_size();
  std::size_t      getnodecount();
  int              get_range_index(std::string_view key);
  auto             seekToFirst();
  auto             seekToLast();
  SkiplistIterator end();
  SkiplistIterator begin();
  SkiplistIterator prefix_serach_begin(std::string_view key);
  SkiplistIterator prefix_serach_end(std::string_view key);
  std::vector<std::tuple<std::string, std::string, uint64_t>> get_prefix_range(
      std::string_view prefix, uint64_t tranc_id);

  void                    set_status(Global_::SkiplistStatus status);
  Global_::SkiplistStatus get_status() const;

 private:
  std::unique_ptr<Node>            head;
  int                              max_level;      // 最大层级
  int                              current_level;  // 当前层级
  std::atomic_size_t               size_bytes;     // 内存占用，达到限制flush到disk
  std::atomic_int                  nodecount;      // 节点数量
  std::random_device               rd;             // 随机数生成器
  static thread_local std::mt19937 gen;            // 随机数引擎
  std::uniform_real_distribution<> dis;            // 随机数分布

  Global_::SkiplistStatus cur_status = Global_::SkiplistStatus::kNormal;
  int                     random_level();
};
