#pragma once
#include "SstableIterator.h"

class TwoMergeIterator;
bool operator==(const TwoMergeIterator& a, const TwoMergeIterator& b);

class TwoMergeIterator : public BaseIterator {
  friend bool operator==(const TwoMergeIterator& a, const TwoMergeIterator& b);

 private:
  std::shared_ptr<BaseIterator>      begin_;
  std::shared_ptr<BaseIterator>      end_;
  mutable std::shared_ptr<valuetype> current;  // 用于存储当前元素
  uint64_t                           max_tranc_id_      = 0;
  bool                               keep_all_versions_ = false;

  void update_current() const;

 public:
  TwoMergeIterator();
  TwoMergeIterator(std::shared_ptr<BaseIterator> begin_, std::shared_ptr<BaseIterator> end_,
                   uint64_t max_tranc_id);
  void                  skip_by_tranc_id();
  auto                  operator<=>(const TwoMergeIterator& other) const;
  virtual BaseIterator& operator++() override;
  virtual valuetype     operator*() const override;
  virtual IteratorType  type() const override;
  virtual uint64_t      get_tranc_id() const override;
  virtual bool          isEnd() const override;
  virtual bool          valid() const override;
};
