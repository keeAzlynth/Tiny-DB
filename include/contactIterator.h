#pragma once

#include "Sstable.h"
#include "SstableIterator.h"
#include <memory>
#include <vector>

class ConcactIterator;
bool operator==(const ConcactIterator& lhs, const ConcactIterator& rhs) noexcept;
class ConcactIterator : public BaseIterator {
  friend bool operator==(const ConcactIterator& lhs, const ConcactIterator& rhs) noexcept;

 private:
  SstIterator cur_iter;
  size_t      cur_idx;  // 不是真实的sst_id, 而是在需要连接的sst数组中的索引
  std::vector<std::shared_ptr<Sstable>> ssts;
  uint64_t                              max_tranc_id_;

 public:
  ConcactIterator(std::vector<std::shared_ptr<Sstable>> ssts, uint64_t tranc_id);

  std::string           key();
  std::string           value();
  auto                  operator<=>(const ConcactIterator& other) const;
  virtual BaseIterator& operator++() override;
  virtual valuetype     operator*() const override;
  virtual IteratorType  type() const override;
  virtual uint64_t      get_tranc_id() const override;
  virtual bool          isEnd() const override;
  virtual bool          valid() const override;

  // pointer operator->() const;
};
