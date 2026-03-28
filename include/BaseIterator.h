#pragma once

#include <cstdint>
#include <string>
#include <utility>
enum class IteratorType {
  SkiplistIterator,
  MemTableIterator,
  BlockIterator,
  SstIterator,
  TwoMergeIterator,
  ConcactIterator,
  LevelIterator,
};

class BaseIterator {
 public:
  using valuetype                     = std::pair<std::string, std::string>;
  using pvaluetype                    = valuetype*;
  using rvaluetype                    = valuetype&;
  virtual ~BaseIterator()             = default;
  virtual bool          valid() const = 0;
  virtual BaseIterator& operator++()  = 0;
  auto                  operator<=>(const BaseIterator& other) const;
  virtual IteratorType  type() const         = 0;
  virtual bool          isEnd() const        = 0;
  virtual valuetype     operator*() const    = 0;
  virtual uint64_t      get_tranc_id() const = 0;
};
class SerachIterator {
 public:
  std::string key_;
  std::string value_;
  uint64_t    transaction_id_;
  size_t      level_;
  size_t      index_;
  SerachIterator(std::string key, std::string value, uint64_t transaction_id, size_t level,
                 size_t index)
      : key_(key), value_(value), transaction_id_(transaction_id), level_(level), index_(index) {}
  SerachIterator()  = default;
  ~SerachIterator() = default;
};
auto operator<=>(const SerachIterator& lhs, const SerachIterator& rhs) -> std::strong_ordering;
