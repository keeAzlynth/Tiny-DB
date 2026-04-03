#include "../include/LSM.h"
#include <algorithm>
#include <array>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include "../include/SstableIterator.h"
#include "../include/LeveIterator.h"
#include "../include/contactIterator.h"
#include "../include/Loger.h"
#include "../include/record.h"
#include "spdlog/spdlog.h"

LSM_Engine::LSM_Engine(std::string path, size_t block_cache_capacity, size_t block_cache_k)
    : data_dir(path),
      memtable(std::make_shared<MemTable>()),
      block_cache(std::make_shared<BlockCache>(block_cache_capacity, block_cache_k)) {
  if (!std::filesystem::exists(path)) {
    std::filesystem::create_directory(path);
  } else {
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      if (!entry.is_regular_file()) {
        continue;
      }

      std::string filename = entry.path().filename().string();
      // SST文件名格式为: sst_{id}.level
      if (!filename.starts_with("sst_")) {
        continue;
      }

      // 找到 . 的位置
      size_t dot_pos = filename.find('.');
      if (dot_pos == std::string::npos || dot_pos == filename.length() - 1) {
        continue;
      }

      // 提取 level
      std::string level_str = filename.substr(dot_pos + 1, filename.length() - 1 - dot_pos);
      if (level_str.empty()) {
        continue;
      }
      size_t level = std::stoull(level_str);

      // 提取SST ID
      std::string id_str = filename.substr(4, dot_pos - 4);  // 4 for "sst_"
      if (id_str.empty()) {
        continue;
      }
      size_t sst_id = std::stoull(id_str);

      // 加载SST文件, 初始化时需要加写锁
      std::unique_lock<std::shared_mutex> lock(ssts_mtx);  // 写锁

      next_sst_id          = std::max(sst_id, next_sst_id);   // 记录目前最大的 sst_id
      cur_max_level        = std::max(level, cur_max_level);  // 记录目前最大的 level
      std::string sst_path = get_sst_path(sst_id, level);
      auto        sst      = Sstable::open(sst_id, FileObj::open(sst_path, false), block_cache);
      ssts[sst_id]         = sst;
      level_sst_ids[level].push_back(sst_id);
    }

    next_sst_id++;  // 现有的最大 sst_id 自增后才是下一个分配的 sst_id

    for (auto& [level, sst_id_list] : level_sst_ids) {
      std::ranges::sort(sst_id_list);
      if (level == 0) {
        // 其他 level 的 sst 都是没有重叠的, 且 id 小的表示最新sst
        // 排序在前面的部分, 不需要 reverse
        std::ranges::reverse(sst_id_list);
      }
    }
  }
}

std::optional<std::pair<std::string, uint64_t>> LSM_Engine::get(const std::string& key,
                                                                uint64_t           tranc_id) {
  // 1. 先查找 memtable
  auto mem_res = memtable->get(key, tranc_id);
  if (mem_res.has_value()) {
    if (mem_res.value().first.empty()) {
      return std::nullopt;  // 空值表示被删除
    }
    return std::pair<std::string, uint64_t>{mem_res.value().first, mem_res.value().second};
  }
  // 2. l0 sst中查询
  std::shared_lock<std::shared_mutex> rlock(ssts_mtx);  // 读锁

  for (auto& sst_id : level_sst_ids[0]) {
    //  中的 sst_id 是按从大到小的顺序排列,
    // sst_id 越大, 表示是越晚刷入的, 优先查询
    auto& sst          = ssts[sst_id];
    auto  sst_iterator = sst->get_Iterator(key, tranc_id);
    if (sst_iterator.valid()) {
      if (sst_iterator->second.empty()) {
        return std::nullopt;  // 空值表示被删除
      }
      return std::pair<std::string, uint64_t>{sst_iterator->second, sst_iterator.get_tranc_id()};
    }
  }

  // 3. 其他level的sst中查询
  for (size_t level = 1; level <= cur_max_level; level++) {
    auto& l_sst_ids = level_sst_ids[level];
    // 二分查询
    size_t left  = 0;
    size_t right = l_sst_ids.size();
    while (left < right) {
      size_t mid = left + (right - left) / 2;
      auto&  sst = ssts[l_sst_ids[mid]];
      if (sst->get_first_key() <= key && key <= sst->get_last_key()) {
        // 如果sst_id在中, 则在sst中查询
        auto sst_iterator = sst->get_Iterator(key, tranc_id);
        if (sst_iterator.valid()) {
          return std::pair<std::string, uint64_t>{sst_iterator->second,
                                                  sst_iterator.get_tranc_id()};
        }
      } else if (sst->get_last_key() < key) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
  }
  return std::nullopt;
}

std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>> LSM_Engine::get_batch(
    const std::vector<std::string>& keys, uint64_t tranc_id_) {
  // 1. 先从 memtable 中批量查找
  auto memtable_results = memtable->get_batch(keys, tranc_id_);

  std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>> results;
  std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>> un_search;

  for (auto& [key, value, tranc_id] : memtable_results) {
    if (!value.has_value()) {
      // memtable 未找到 → 加入待查列表，不加入 results
      un_search.emplace_back(key, std::string(), tranc_id_);
    } else if (value.value().empty()) {
      // 空字符串 = 删除标记
      results.emplace_back(key, std::nullopt, tranc_id);
    } else {
      results.emplace_back(key, value, tranc_id);
    }
  }

  if (un_search.empty()) {
    return results;
  }

  // 2. 从 L0 层 SST 文件中批量查找未命中的键
  std::shared_lock<std::shared_mutex> rlock(ssts_mtx);
  for (auto& sst_id : level_sst_ids[0]) {
    auto& sst = ssts[sst_id];
    for (auto& [k, v, tranc_id] : un_search) {
      if (v.has_value() && v.value().empty()) {  // 仍未找到
        auto res = sst->get_Iterator(k, tranc_id);
        if (res.valid()) {
          auto val_str   = res->second;
          auto val_tranc = res.get_tranc_id();
          if (val_str.empty()) {
            v        = std::nullopt;
            tranc_id = val_tranc;
          } else {
            v        = val_str;
            tranc_id = val_tranc;
          }
        }
      }
    }
  }

  // 将 L0 已找到的移入 results，未找到的进入 un_search_L0
  std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>> un_search_L0;
  for (auto& item : un_search) {
    auto& v = std::get<1>(item);
    if (v.has_value() && v.value().empty()) {
      // L0 也未找到
      un_search_L0.emplace_back(std::get<0>(item), std::string(), tranc_id_);
    } else {
      results.emplace_back(item);
    }
  }

  // 3. 对每一层（L1 到 cur_max_level）
  for (size_t level = 1; level <= cur_max_level && !un_search_L0.empty(); ++level) {
    const auto& sst_ids = level_sst_ids[level];
    if (sst_ids.empty())
      continue;

    // 为每个待查找的键，二分找出它可能所在的 SST
    std::vector<std::pair<size_t, size_t>> key_to_sst_idx;
    key_to_sst_idx.reserve(un_search_L0.size());

    for (size_t i = 0; i < un_search_L0.size(); ++i) {
      const auto& key = std::get<0>(un_search_L0[i]);
      if (!std::get<1>(un_search_L0[i]).has_value() ||
          !std::get<1>(un_search_L0[i]).value().empty()) {
        continue;  // 已在上一层找到，跳过
      }
      size_t left = 0, right = sst_ids.size();
      while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (ssts[sst_ids[mid]]->get_first_key() <= key) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
      if (left == 0)
        continue;
      size_t      idx = left - 1;
      const auto& sst = ssts[sst_ids[idx]];
      if (key <= sst->get_last_key()) {
        key_to_sst_idx.emplace_back(i, idx);
      }
    }

    // 按 SST 分组
    std::sort(key_to_sst_idx.begin(), key_to_sst_idx.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });

    // 遍历每个 SST，批量查找属于它的键
    for (size_t g = 0; g < key_to_sst_idx.size();) {
      size_t sst_idx = key_to_sst_idx[g].second;
      size_t sst_id  = sst_ids[sst_idx];
      auto&  sst     = ssts[sst_id];

      std::vector<size_t> batch_keys_idx;
      while (g < key_to_sst_idx.size() && key_to_sst_idx[g].second == sst_idx) {
        batch_keys_idx.push_back(key_to_sst_idx[g].first);
        ++g;
      }

      for (size_t ki : batch_keys_idx) {
        auto& [key, val, tranc] = un_search_L0[ki];
        if (val.has_value() && val.value().empty()) {  // 仍未找到
          auto iter = sst->get_Iterator(key, tranc);
          if (iter.valid()) {
            auto val_str = iter->second;
            tranc        = iter.get_tranc_id();
            val          = val_str.empty() ? std::nullopt : std::optional<std::string>(val_str);
          }
        }
      }
    }
  }

  // 4. 将 un_search_L0 的结果合并进 results
  for (auto& [key, val, tranc] : un_search_L0) {
    if (val.has_value() && val.value().empty()) {
      // 所有层都未找到 → key 不存在
      results.emplace_back(key, std::nullopt, tranc);
    } else {
      results.emplace_back(key, val, tranc);
    }
  }

  return results;
}
std::optional<std::pair<std::string, uint64_t>> LSM_Engine::sst_get_(const std::string& key,
                                                                     uint64_t           tranc_id) {
  // 1. l0 sst中查询
  for (auto& sst_id : level_sst_ids[0]) {
    //  中的 sst_id 是按从大到小的顺序排列,
    // sst_id 越大, 表示是越晚刷入的, 优先查询
    auto sst          = ssts[sst_id];
    auto sst_iterator = sst->get_Iterator(key, tranc_id);
    if (sst_iterator.valid()) {
      if (!sst_iterator->second.empty()) {
        return std::pair<std::string, uint64_t>{sst_iterator->second, sst_iterator.get_tranc_id()};
      } else {
        // 空值表示被删除了
        return std::nullopt;
      }
    }
  }

  // 2. 其他level的sst中查询
  for (size_t level = 1; level <= cur_max_level; level++) {
    std::deque<size_t> l_sst_ids = level_sst_ids[level];
    // 二分查询
    size_t left  = 0;
    size_t right = l_sst_ids.size();
    while (left < right) {
      size_t mid = left + (right - left) / 2;
      auto   sst = ssts[l_sst_ids[mid]];
      if (sst->get_first_key() <= key && key <= sst->get_last_key()) {
        // 如果sst_id在中, 则在sst中查询
        auto sst_iterator = sst->get_Iterator(key, tranc_id);
        if (sst_iterator.valid()) {
          return std::pair<std::string, uint64_t>{sst_iterator->second,
                                                  sst_iterator.get_tranc_id()};
        }
      } else if (sst->get_last_key() < key) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
  }
  return std::nullopt;
}

uint64_t LSM_Engine::put(const std::string& key, const std::string& value, uint64_t tranc_id) {
  memtable->put_mutex(key, value, tranc_id);
  // 如果 memtable 太大，需要刷新到磁盘
  if (memtable->get_total_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    return flush();
  }
  return 0;
}

uint64_t LSM_Engine::put_batch(const std::vector<std::pair<std::string, std::string>>& kvs,
                               uint64_t                                                tranc_id) {
  memtable->put_batch(kvs, tranc_id);
  // 如果 memtable 太大，需要刷新到磁盘
  if (memtable->get_total_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    return flush();
  }
  return 0;
}
uint64_t LSM_Engine::remove(const std::string& key, uint64_t tranc_id) {
  // 在 LSM 中，删除实际上是插入一个空值
  memtable->remove(key, tranc_id);
  // 如果 memtable 太大，需要刷新到磁盘
  if (memtable->get_total_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    return flush();
  }
  return 0;
}

uint64_t LSM_Engine::remove_batch(const std::vector<std::string>& keys, uint64_t tranc_id) {
  memtable->remove_batch(keys, tranc_id);
  // 如果 memtable 太大，需要刷新到磁盘
  if (memtable->get_total_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE) {
    return flush();
  }
  return 0;
}

void LSM_Engine::clear() {
  level_sst_ids.clear();
  ssts.clear();
  memtable->clear();
  // 清空当前文件夹的所有内容
  try {
    for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      std::filesystem::remove(entry.path());
    }
  } catch (const std::filesystem::filesystem_error& e) {
    // 处理文件系统错误
    spdlog::error("Error clearing directory: {}", e.what());
  }
}

uint64_t LSM_Engine::flush() {
  if (memtable->get_total_size() == 0) {
    return 0;
  }

  std::unique_lock<std::shared_mutex> lock(ssts_mtx);  // 写锁

  // 1. 先判断 l0 sst 是否数量超限需要concat到 l1
  if (level_sst_ids.find(0) != level_sst_ids.end() &&
      level_sst_ids[0].size() >= Global_::LSM_SST_LEVEL_RATIO) {
    full_compact(0);
  }

  // 2. 创建新的 SST ID
  size_t new_sst_id = next_sst_id++;

  // 3. 准备 SSTBuilder
  Sstbuild builder(Global_::Block_SIZE,
                   true);  // 4KB block size

  // 4. 将 memtable 中最旧的表写入 SST
  std::vector<uint64_t> flushed_tranc_ids;
  auto                  sst_path = get_sst_path(new_sst_id, 0);
  if (memtable->get_cur_size() > 0) {
    memtable->frozen_cur_table();
  }
  auto res = memtable->flushtodisk();
  for (auto i = res->begin(); i != res->end(); ++i) {
    auto kv       = i.getValue();
    auto tranc_id = i.get_tranc_id();
    builder.add(kv.first, kv.second, tranc_id);
  }
  auto new_sst = builder.build(block_cache, sst_path, new_sst_id);
  if (!new_sst) {
    next_sst_id--;
    return 0;
  }
  // 5. 更新内存索引
  ssts[new_sst_id] = new_sst;

  // 6. 更新 sst_ids
  level_sst_ids[0].push_front(new_sst_id);

  // 7. 添加到 flushed 集合
  for (auto& id : flushed_tranc_ids) {
    tran_manager.lock()->add_flushed_tranc_id(id);
  }
  return new_sst->get_tranc_id_range().second;
}

std::string LSM_Engine::get_sst_path(size_t sst_id, size_t target_level) {
  // sst的文件路径格式为: data_dir/sst_<sst_id>，sst_id格式化为32位数字
  std::stringstream ss;
  ss << data_dir << "/sst_" << std::setfill('0') << std::setw(32) << sst_id << '.' << target_level;
  return ss.str();
}

Level_Iterator LSM_Engine::begin(uint64_t tranc_id) {
  return Level_Iterator(shared_from_this(), tranc_id);
}

Level_Iterator LSM_Engine::end() {
  return Level_Iterator{};
}

bool LSM_Engine::exit_valid_sst_iter(std::vector<SstIterator>& sst_iters) {
  for (auto& it : sst_iters) {
    if (it.valid()) {
      return true;
    }
  }
  return false;
}

std::pair<size_t, size_t> LSM_Engine::find_the_small_kv(std::vector<SstIterator>& sst_iters) {
  std::size_t index{0};
  // 先移除所有无效迭代器
  auto valid_end =
      std::ranges::remove_if(sst_iters, [&](const SstIterator& it) { return !it.valid(); });
  sst_iters.erase(valid_end.begin(), valid_end.end());

  // 然后在剩余的有效迭代器中找最小值（此时比较函数可以简化，因为都有效）
  auto res = std::ranges::min_element(sst_iters, [&](const SstIterator& a, const SstIterator& b) {
    if (a.key() != b.key())
      return a.key() < b.key();
    index++;
    return a.get_tranc_id() > b.get_tranc_id();
  });
  return std::make_pair(res - sst_iters.begin(), index);
}
void LSM_Engine::full_compact(size_t src_level) {
  // 将 src_level 的 sst 全体压缩到 src_level + 1
  // 获取源level和目标level的 sst_id
  auto                                  old_level_id_x = level_sst_ids[src_level];
  auto                                  old_level_id_y = level_sst_ids[src_level + 1];
  std::vector<std::shared_ptr<Sstable>> new_ssts;
  std::vector<size_t>                   lx_ids(old_level_id_x.begin(), old_level_id_x.end());
  std::vector<size_t>                   ly_ids(old_level_id_y.begin(), old_level_id_y.end());
  if (src_level == 0) {
    // l0这一层不同sst的key有重叠, 需要额外处理
    new_ssts = std::move(full_l0_l1_compact(lx_ids, ly_ids));
  } else {
    new_ssts = std::move(full_common_compact(lx_ids, ly_ids, src_level + 1));
  }
  // 完成 compact 后移除旧的sst记录
  for (auto& old_sst_id : old_level_id_x) {
    ssts[old_sst_id]->del_sst();
    ssts.erase(old_sst_id);
  }
  for (auto& old_sst_id : old_level_id_y) {
    ssts[old_sst_id]->del_sst();
    ssts.erase(old_sst_id);
  }
  level_sst_ids[src_level].clear();
  level_sst_ids[src_level + 1].clear();

  cur_max_level = std::max(cur_max_level, src_level + 1);

  // 添加新的sst
  for (auto& new_sst : new_ssts) {
    level_sst_ids[src_level + 1].push_back(new_sst->get_sst_id());
    ssts[new_sst->get_sst_id()] = new_sst;
  }
  // 此处没必要reverse了
  std::sort(level_sst_ids[src_level + 1].begin(), level_sst_ids[src_level + 1].end());
  // 递归地判断下一级 level 是否需要 full compact
  if (level_sst_ids[src_level + 1].size() >= Global_::LSM_SST_LEVEL_RATIO) {
    full_compact(src_level + 1);
  }
}

std::vector<std::shared_ptr<Sstable>> LSM_Engine::full_l0_l1_compact(std::vector<size_t>& l0_ids,
                                                                     std::vector<size_t>& l1_ids) {
  std::vector<std::shared_ptr<Sstable>> sst;
  sst.reserve(std::max(l0_ids.size(), l1_ids.size()) + 1);
  auto merged  = merge_sst_iterator(l0_ids, l1_ids);
  auto builder = std::make_unique<Sstbuild>(Global_::Block_CACHE_capacity);

  auto advance_all_with_key = [&](const std::string& key) {
    for (auto& it : merged) {
      while (it.valid() && it.key() == key) {
        ++it;
      }
    }
  };

  auto advance_one = [&](size_t idx, const std::string& key) {
    while (merged[idx].valid() && merged[idx].key() == key) {
      ++merged[idx];
    }
  };

  auto find_best = [&]() -> std::pair<size_t, bool> {
    std::string min_key;
    uint64_t    max_tranc = 0;
    size_t      best_idx  = SIZE_MAX;
    bool        has_dup   = false;

    for (size_t i = 0; i < merged.size(); ++i) {
      if (!merged[i].valid())
        continue;
      const auto& k = merged[i].key();
      if (best_idx == SIZE_MAX || k < min_key) {
        min_key   = k;
        max_tranc = merged[i].get_tranc_id();
        best_idx  = i;
        has_dup   = false;
      } else if (k == min_key) {
        has_dup = true;
        if (merged[i].get_tranc_id() > max_tranc) {
          max_tranc = merged[i].get_tranc_id();
          best_idx  = i;
        }
      }
    }
    return {best_idx, has_dup};
  };

  while (exit_valid_sst_iter(merged)) {
    auto [best_idx, has_dup] = find_best();
    if (best_idx == SIZE_MAX)
      break;

    std::string cur_key = merged[best_idx].key();
    builder->add(cur_key, merged[best_idx].value(), merged[best_idx].get_tranc_id());

    if (has_dup) {
      advance_all_with_key(cur_key);
    } else {
      advance_one(best_idx, cur_key);
    }

    if (builder->estimated_size() >= Global_::MAX_SSTABLE_SIZE) {
      size_t new_id = next_sst_id++;
      sst.emplace_back(builder->build(block_cache, get_sst_path(new_id, 1), new_id));
      builder = std::make_unique<Sstbuild>(Global_::Block_CACHE_capacity);
    }
  }

  if (builder->estimated_size() > 0) {
    size_t new_id = next_sst_id++;
    sst.emplace_back(builder->build(block_cache, get_sst_path(new_id, 1), new_id));
  }

  return sst;
}
std::vector<std::shared_ptr<Sstable>> LSM_Engine::full_common_compact(std::vector<size_t>& lx_ids,
                                                                      std::vector<size_t>& ly_ids,
                                                                      size_t level_y) {
  std::vector<std::shared_ptr<Sstable>> sst;
  sst.reserve(std::max(lx_ids.size(), ly_ids.size()) + 1);
  auto merged  = merge_sst_iterator(lx_ids, ly_ids);
  auto builder = std::make_unique<Sstbuild>(Global_::Block_CACHE_capacity);

  auto advance_all_with_key = [&](const std::string& key) {
    for (auto& it : merged) {
      while (it.valid() && it.key() == key) {
        ++it;
      }
    }
  };

  auto advance_one = [&](size_t idx, const std::string& key) {
    while (merged[idx].valid() && merged[idx].key() == key) {
      ++merged[idx];
    }
  };

  auto find_best = [&]() -> std::pair<size_t, bool> {
    std::string min_key;
    uint64_t    max_tranc = 0;
    size_t      best_idx  = SIZE_MAX;
    bool        has_dup   = false;

    for (size_t i = 0; i < merged.size(); ++i) {
      if (!merged[i].valid())
        continue;
      const auto& k = merged[i].key();
      if (best_idx == SIZE_MAX || k < min_key) {
        min_key   = k;
        max_tranc = merged[i].get_tranc_id();
        best_idx  = i;
        has_dup   = false;
      } else if (k == min_key) {
        has_dup = true;
        if (merged[i].get_tranc_id() > max_tranc) {
          max_tranc = merged[i].get_tranc_id();
          best_idx  = i;
        }
      }
    }
    return {best_idx, has_dup};
  };

  while (exit_valid_sst_iter(merged)) {
    auto [best_idx, has_dup] = find_best();
    if (best_idx == SIZE_MAX)
      break;

    std::string cur_key = merged[best_idx].key();
    builder->add(cur_key, merged[best_idx].value(), merged[best_idx].get_tranc_id());

    if (has_dup) {
      advance_all_with_key(cur_key);
    } else {
      advance_one(best_idx, cur_key);
    }

    if (builder->estimated_size() >= Global_::MAX_SSTABLE_SIZE) {
      size_t new_id = next_sst_id++;
      sst.emplace_back(builder->build(block_cache, get_sst_path(new_id, level_y), new_id));
      builder = std::make_unique<Sstbuild>(Global_::Block_CACHE_capacity);
    }
  }

  if (builder->estimated_size() > 0) {
    size_t new_id = next_sst_id++;
    sst.emplace_back(builder->build(block_cache, get_sst_path(new_id, level_y), new_id));
  }

  return sst;
}

std::vector<std::shared_ptr<Sstable>> LSM_Engine::gen_sst_from_iter(BaseIterator& iter,
                                                                    size_t        target_sst_size,
                                                                    size_t        target_level) {
  std::vector<std::shared_ptr<Sstable>> new_ssts;
  auto                                  new_sst_builder = Sstbuild(Global_::Block_SIZE, true);
  while (iter.valid() && !iter.isEnd()) {
    new_sst_builder.add((*iter).first, (*iter).second, 0);
    ++iter;

    if (new_sst_builder.estimated_size() >= target_sst_size) {
      size_t      sst_id   = next_sst_id++;
      std::string sst_path = get_sst_path(sst_id, target_level);
      auto        new_sst  = new_sst_builder.build(block_cache, sst_path, sst_id);
      new_ssts.push_back(new_sst);

      new_sst_builder = Sstbuild(Global_::Block_SIZE, true);
    }
  }
  if (new_sst_builder.estimated_size() > 0) {
    size_t      sst_id   = next_sst_id++;
    std::string sst_path = get_sst_path(sst_id, target_level);
    auto        new_sst  = new_sst_builder.build(block_cache, sst_path, sst_id);
    new_ssts.push_back(new_sst);
  }
  return new_ssts;
}
size_t LSM_Engine::get_sst_size(size_t level) {
  if (level == 0) {
    return Global_::MAX_MEMTABLE_SIZE_PER_TABLE;
  } else {
    return Global_::MAX_MEMTABLE_SIZE_PER_TABLE *
           static_cast<size_t>(std::pow(Global_::LSM_SST_LEVEL_RATIO, level));
  }
}

std::vector<SstIterator> LSM_Engine::merge_sst_iterator(std::vector<std::size_t> iter_id0,
                                                        std::vector<std::size_t> iter_id1) {
  if (iter_id0.empty() && iter_id1.empty()) {
    return std::vector<SstIterator>{};
  }
  std::vector<SstIterator> l0_l1_iters;
  l0_l1_iters.reserve(iter_id0.size() + iter_id1.size());
  for (auto id : iter_id0) {
    auto sst_it = ssts[id]->begin(0);
    l0_l1_iters.push_back(sst_it);
  }
  for (auto id : iter_id1) {
    l0_l1_iters.push_back(ssts[id]->begin(0));
  }
  return l0_l1_iters;
}
void LSM_Engine::set_tran_manager(std::shared_ptr<TranManager> tran_manager_) {
  tran_manager = tran_manager_;
}

LSM::LSM(std::string path)
    : engine(std::make_shared<LSM_Engine>(path)),
      tran_manager_(std::make_shared<TranManager>(path)) {
  tran_manager_->set_engine(engine);
  engine->set_tran_manager(tran_manager_);
  auto check_recover_res = tran_manager_->check_recover();
  for (auto& [tranc_id, records] : check_recover_res) {
    if (tran_manager_->get_flushed_tranc_ids().count(tranc_id)) {
      continue;
    }
    for (auto& record : records) {
      if (record.getOperationType() == OperationType::PUT) {
        engine->put(record.getKey(), record.getValue(), tranc_id);
      } else if (record.getOperationType() == OperationType::DELETE) {
        engine->remove(record.getKey(), tranc_id);
      }
    }
    tran_manager_->init_new_wal();
  }
}
LSM::~LSM() {
  flush_all();
  tran_manager_->write_tranc_id_file();
}

std::optional<std::string> LSM::get(const std::string& key) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  auto res      = engine->get(key, tranc_id);

  if (res.has_value()) {
    return res.value().first;
  }
  return std::nullopt;
}

std::vector<std::pair<std::string, std::optional<std::string>>> LSM::get_batch(
    const std::vector<std::string>& keys) {
  // 1. 获取事务ID
  auto tranc_id = tran_manager_->getNextTransactionId();

  // 2. 调用 engine 的批量查询接口
  auto batch_results = engine->get_batch(keys, tranc_id);

  // 3. 构造最终结果
  std::vector<std::pair<std::string, std::optional<std::string>>> results;
  for (const auto& [key, value, tr] : batch_results) {
    if (value.has_value()) {
      results.emplace_back(key, value);  // 提取值部分
    } else {
      results.emplace_back(key, std::nullopt);  // 键不存在
    }
  }

  return results;
}

void LSM::put(const std::string& key, const std::string& value) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->put(key, value, tranc_id);
}

void LSM::put_batch(const std::vector<std::pair<std::string, std::string>>& kvs) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->put_batch(kvs, tranc_id);
}
void LSM::remove(const std::string& key) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->remove(key, tranc_id);
}

void LSM::remove_batch(const std::vector<std::string>& keys) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->remove_batch(keys, tranc_id);
}

void LSM::clear() {
  engine->clear();
}

void LSM::flush() {
  auto max_tranc_id = engine->flush();
}

void LSM::flush_all() {
  while (engine->memtable->get_total_size() > 0) {
    auto max_tranc_id = engine->flush();
    // tran_manager_->update_checkpoint_tranc_id(max_tranc_id);
  }
}

LSM::LSMIterator LSM::begin(uint64_t tranc_id) {
  return engine->begin(tranc_id);
}

LSM::LSMIterator LSM::end() {
  return engine->end();
}

// 开启一个事务
std::shared_ptr<TranContext> LSM::begin_tran(const IsolationLevel& isolation_level) {
  auto tranc_context = tran_manager_->new_tranc(isolation_level);
  return tranc_context;
}

void LSM::set_log_level(const std::string& level) {
  reset_log_level(level);
}
