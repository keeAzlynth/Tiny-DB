#pragma once
#include <set>

template <class T>
std::set<T> compressSet(const std::set<T>& s) {
  if (s.empty())
    return std::set<T>();

  std::set<T> result;
  auto        it   = s.begin();
  T           last = *it;
  ++it;
  while (it != s.end()) {
    if (*it == last + 1) {
      last = *it;
    } else {
      break;
    }
    ++it;
  }
  result.insert(last);
  while (it != s.end()) {
    result.insert(*it);
    ++it;
  }
  return result;
}