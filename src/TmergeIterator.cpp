#include "../include/TmergeIterator.h"

bool operator==(const TwoMergeIterator& lhs, const TwoMergeIterator& rhs) {
  return lhs.begin_ == rhs.begin_ && lhs.end_ == rhs.end_;
}
TwoMergeIterator::TwoMergeIterator() {}

TwoMergeIterator::TwoMergeIterator(std::shared_ptr<BaseIterator> begin_,
                                   std::shared_ptr<BaseIterator> end_, uint64_t max_tranc_id)
    : begin_(begin_), end_(end_), max_tranc_id_(max_tranc_id) {
  // 先跳过不可见的事务
  skip_by_tranc_id();
}

void TwoMergeIterator::skip_by_tranc_id() {
  if (max_tranc_id_ == 0) {
    return;
  }
  while (begin_->get_tranc_id() > max_tranc_id_) {
    ++(*begin_);
  }
}

BaseIterator& TwoMergeIterator::operator++() {
  ++(*begin_);
  // 先跳过不可见的事务
  skip_by_tranc_id();
  // 跳过重复的 key
  return *this;
}

BaseIterator::valuetype TwoMergeIterator::operator*() const {
  return **begin_;
}

IteratorType TwoMergeIterator::type() const {
  return IteratorType::TwoMergeIterator;
}

uint64_t TwoMergeIterator::get_tranc_id() const {
  if (valid()) {
    return begin_->get_tranc_id();
  }
  return max_tranc_id_;
}

bool TwoMergeIterator::isEnd() const {
  return begin_ == nullptr ? true : false;
}

bool TwoMergeIterator::valid() const {
  return !isEnd();
}

void TwoMergeIterator::update_current() const {
  current = std::make_shared<valuetype>(**begin_);
}
