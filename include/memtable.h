#pragma once
#include "Skiplist.h"
#include <atomic>
#include <cstddef>
#include <list>
#include <memory>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>

class MemTableIterator;
bool operator==(const MemTableIterator& lhs, const MemTableIterator& rhs) noexcept;

class MemTableIterator : public BaseIterator {
 public:
  friend class SstIterator;
  friend class LSM_Engine;

  friend bool operator==(const MemTableIterator& lhs, const MemTableIterator& rhs) noexcept;
  using valuetype = std::pair<std::string, std::string>;
  MemTableIterator();
  MemTableIterator(std::vector<SerachIterator> iter, const uint64_t max_transaction_id);
  MemTableIterator(const SkiplistIterator& iter, const uint64_t max_transaction_id);
  ~MemTableIterator() = default;

  bool valid() const override;

  auto              operator<=>(const MemTableIterator& other) const;
  valuetype         operator*() const override;
  pvaluetype        operator->() const;
  MemTableIterator& operator++() override;

  bool         isEnd() const override;
  uint64_t     get_tranc_id() const override;
  IteratorType type() const override;
  valuetype    getValue() const;
  void         pop_value();
  void         update_current_key_value() const;

 private:
  bool                               top_value_legal() const;
  void                               skip_transaction_id();
  mutable std::shared_ptr<valuetype> current_value_;
  std::shared_ptr<SkiplistIterator>  list_iter_;
  std::priority_queue<SerachIterator, std::vector<SerachIterator>, std::greater<SerachIterator>>
           queue_;
  uint64_t max_transaction_id;
};

class MemTable {
  friend class MemTableIterator;  // 让 MemTableIterator 可以访问私有成员
  friend class TranContext;

 public:
  MemTable();
  MemTable(const MemTable& other)            = delete;
  MemTable& operator=(const MemTable& other) = delete;
  ~MemTable()                                = default;
  std::vector<std::tuple<std::string, std::string, uint64_t>> get_prefix_range(
      const std::string& prefix, uint64_t tranc_id);
  void clear();
  void put(const std::string& key, const std::string& value, const uint64_t transaction_id = 0);
  void put_mutex(const std::string& key, const std::string& value,
                 const uint64_t transaction_id = 0);
  void put_batch(const std::vector<std::pair<std::string, std::string>>& key_value_pairs,
                 const uint64_t                                          transaction_id = 0);
  std::optional<std::pair<std::string, uint64_t>> get(const std::string& key,
                                                      const uint64_t     transaction_id = 0);

  SkiplistIterator cur_get(const std::string& key, const uint64_t transaction_id = 0);
  SkiplistIterator fix_get(const std::string& key, const uint64_t transaction_id = 0);
  std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>> get_batch(
      const std::vector<std::string>& key_s, const uint64_t transaction_id = 0);
  size_t get_node_num() const;
  size_t get_cur_size();
  size_t get_fixed_size();
  size_t get_total_size();
  void   remove(const std::string& key, const uint64_t transaction_id = 0);
  void   remove_mutex(const std::string& key, const uint64_t transaction_id = 0);
  void   remove_batch(const std::vector<std::string>& key_pairs, const uint64_t transaction_id = 0);
  bool   IsFull();
  std::unique_ptr<Skiplist>            flushtodisk();
  std::unique_ptr<Skiplist>            flush();
  std::list<std::unique_ptr<Skiplist>> flushsync();
  void                                 frozen_cur_table();
  MemTableIterator                     begin();
  MemTableIterator                     end();
  MemTableIterator prefix_serach(const std::string& key, const uint64_t transaction_id = 0);

 private:
  std::unique_ptr<Skiplist>            current_table;  // 活跃 SkipList
  std::list<std::unique_ptr<Skiplist>> fixed_tables;   // 不可写的 SkipList==InmutTable
  std::atomic_size_t                   fixed_bytes;    // fixed_tables的跳表的大小
  std::shared_mutex                    fix_lock_;
  std::shared_mutex                    cur_lock_;   // 保护当前跳表的锁
  std::atomic<Global_::SkiplistStatus> cur_status;  // 当前跳表的状态
};