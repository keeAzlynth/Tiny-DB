#include "../include/contactIterator.h"

bool operator==(const ConcactIterator& lhs, const ConcactIterator& rhs) noexcept {
  if (lhs.cur_idx != rhs.cur_idx) {
    return false;
  }
  if (lhs.cur_idx >= lhs.ssts.size() || rhs.cur_idx >= rhs.ssts.size()) {
    return true;
  }
  return lhs.cur_iter == rhs.cur_iter;
}

ConcactIterator::ConcactIterator(std::vector<std::shared_ptr<Sstable>> ssts, uint64_t tranc_id)
    : ssts(ssts), cur_iter(nullptr, tranc_id), cur_idx(0), max_tranc_id_(tranc_id) {
  if (!ssts.empty()) {
    cur_iter = ssts[0]->begin(max_tranc_id_);
  }
}

auto ConcactIterator::operator<=>(const ConcactIterator& other) const {
  if (cur_idx != other.cur_idx) {
    return cur_idx < other.cur_idx ? std::strong_ordering::less : std::strong_ordering::greater;
  }
  if (cur_idx >= ssts.size() || other.cur_idx >= other.ssts.size()) {
    return std::strong_ordering::equal;
  }
  return cur_iter <=> other.cur_iter;
}
BaseIterator& ConcactIterator::operator++() {
  ++cur_iter;

  if (!cur_iter.valid()) {
    cur_idx++;
    if (cur_idx < ssts.size()) {
      cur_iter = ssts[cur_idx]->begin(max_tranc_id_);
    } else {
      cur_iter = SstIterator(nullptr, max_tranc_id_);
    }
  }
  return *this;
}

ConcactIterator::valuetype ConcactIterator::operator*() const {
  return *cur_iter;
}

IteratorType ConcactIterator::type() const {
  return IteratorType::ConcactIterator;
}

uint64_t ConcactIterator::get_tranc_id() const {
  return cur_iter.get_tranc_id();
}

bool ConcactIterator::isEnd() const {
  return cur_iter.isEnd();
}

bool ConcactIterator::valid() const {
  return cur_iter.valid();
}

std::string ConcactIterator::key() {
  return cur_iter.key();
}

std::string ConcactIterator::value() {
  return cur_iter.value();
}
