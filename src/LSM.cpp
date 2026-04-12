#include "../include/LSM.h"
#include <algorithm>
#include <array>
#include <cmath>
#include <filesystem>
#include <memory>
#include <optional>
#include <print>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
#include "../include/iterator/SstableIterator.h"
#include "../include/iterator/LeveIterator.h"
#include "../include/iterator/contactIterator.h"
#include "../include/utils/Loger.h"
#include "../include/core/record.h"
#include "spdlog/spdlog.h"

// ════════════════════════════════════════════════════════════════════════════
//  LSM_Engine  — construction / destruction
// ════════════════════════════════════════════════════════════════════════════

LSM_Engine::LSM_Engine(std::string path, size_t block_cache_capacity, size_t block_cache_k)
    : data_dir(path),
      memtable(std::make_shared<MemTable>()),
      level_size{0},
      block_cache(std::make_shared<BlockCache>(block_cache_capacity, block_cache_k)) {

  if (!std::filesystem::exists(path))
    std::filesystem::create_directory(path);

  // ── 1. Load (or create) the MANIFEST ─────────────────────────────────────
  //  Manifest is constructed first so we can derive the WAL checkpoint from
  //  the maximum tranc_id of all SSTs that have already been flushed to disk.
  manifest_ = std::make_unique<Manifest>(path);

  // ── 2. Reconstruct SST index ──────────────────────────────────────────────
  if (manifest_->was_loaded()) {
    // ── 2a. MANIFEST present: trust it as the source of truth ─────────────
    for (const auto& meta : manifest_->get_live_ssts()) {
      const size_t sst_id   = meta.sst_id;
      const size_t level    = meta.level;
      std::string  sst_path = get_sst_path(sst_id, level);

      if (!std::filesystem::exists(sst_path)) {
        spdlog::warn("MANIFEST references missing SST file: {} — skipping", sst_path);
        continue;
      }

      auto sst      = Sstable::open(sst_id, FileObj::open(sst_path, false), block_cache);
      ssts[sst_id]  = sst;
      level_sst_ids[level].push_back(sst_id);
      level_size[level] += sst->get_sst_size();
      next_sst_id   = std::max(sst_id + 1, next_sst_id.load());
      cur_max_level = std::max(level, cur_max_level);
    }
  } else {
    // ── 2b. First run or MANIFEST missing: scan directory + backfill ───────
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      if (!entry.is_regular_file()) continue;

      const std::string filename = entry.path().filename().string();
      if (!filename.starts_with("sst_")) continue;

      const size_t dot_pos = filename.find('.');
      if (dot_pos == std::string::npos || dot_pos == filename.length() - 1) continue;

      const std::string level_str = filename.substr(dot_pos + 1);
      const std::string id_str    = filename.substr(4, dot_pos - 4);
      if (level_str.empty() || id_str.empty()) continue;

      const size_t level  = std::stoull(level_str);
      const size_t sst_id = std::stoull(id_str);

      next_sst_id   = std::max(sst_id, next_sst_id.load());
      cur_max_level = std::max(level, cur_max_level);

      const std::string sst_path = get_sst_path(sst_id, level);
      auto sst = Sstable::open(sst_id, FileObj::open(sst_path, false), block_cache);
      ssts[sst_id]          = sst;
      level_sst_ids[level].push_back(sst_id);
      level_size[level]    += sst->get_sst_size();

      // Backfill MANIFEST so future restarts can use it.
      auto [min_tid, max_tid] = sst->get_tranc_id_range();
      manifest_->add_sst(SstMeta{
          .sst_id       = sst_id,
          .level        = level,
          .min_tranc_id = min_tid,
          .max_tranc_id = max_tid,
          .first_key    = sst->get_first_key(),
          .last_key     = sst->get_last_key(),
      });
    }
    // Allocate next SST id past the highest found.
    next_sst_id++;
  }

  // ── Fix up per-level ordering ─────────────────────────────────────────────
  for (auto& [level, sst_id_list] : level_sst_ids) {
    std::ranges::sort(sst_id_list);
    if (level == 0)
      // L0: newer SSTs (higher id) must be searched first.
      std::ranges::reverse(sst_id_list);
  }

  // ── 3. Create WAL with checkpoint derived from MANIFEST ───────────────────
  //  checkpoint_tranc_id() == max(max_tranc_id of all flushed SSTs).
  //  WAL::recover will only replay entries NEWER than this value.
  const uint64_t checkpoint = manifest_->checkpoint_tranc_id();
  wal = std::make_unique<WAL>(path, checkpoint);

  // ── 4. Replay WAL entries newer than checkpoint into memtable ─────────────
  uint64_t max_recovered = checkpoint;
  if (auto recovered = WAL::recover(path, checkpoint); recovered.has_value()) {
    for (auto& [tranc_id, entries] : recovered.value()) {
      for (auto& e : entries) {
        if (e.value.empty())
          memtable->remove(e.key, e.tranc_id);       // tombstone
        else
          memtable->put_mutex(e.key, e.value, e.tranc_id);
        max_recovered = std::max(max_recovered, e.tranc_id);
      }
    }
    if (!recovered.value().empty())
      spdlog::info("WAL recovery: replayed entries up to tranc_id={}", max_recovered);
  } else {
    spdlog::error("WAL recovery returned checksum error — some data may be lost");
  }

  // ── 5. Advance transaction counter past all known ids ────────────────────
  //  Must be strictly greater than any tranc_id ever written, so new
  //  transactions cannot collide with recovered data.
  nextTransactionId_.store(max_recovered + 1, std::memory_order_relaxed);

  // ── 6. Start background compaction thread ─────────────────────────────────
  //  Started last so that wal, manifest_, and ssts are fully initialised
  //  before the thread can call flush().
  compaction_thread_ = std::thread(&LSM_Engine::compaction_worker, this);
}

LSM_Engine::~LSM_Engine() {
  {
    std::lock_guard lk(compaction_mutex_);
    stop_compaction_ = true;
  }
  compaction_cv_.notify_one();
  if (compaction_thread_.joinable())
    compaction_thread_.join();
}

// ════════════════════════════════════════════════════════════════════════════
//  Read paths  (unchanged from original)
// ════════════════════════════════════════════════════════════════════════════

std::vector<std::tuple<std::string, std::string, uint64_t>> LSM_Engine::get_prefix_range(
    const std::string& prefix, uint64_t tranc_id_) {
  std::unordered_map<std::string, std::pair<std::string, uint64_t>> merged;

  auto memtable_results = memtable->get_prefix_range(prefix, tranc_id_);
  for (auto& [k, v, tranc_id] : memtable_results)
    merged[k] = {v, tranc_id};

  std::shared_lock<std::shared_mutex> rlock(ssts_mtx);

  for (auto& sst_id : level_sst_ids[0]) {
    auto& sst     = ssts[sst_id];
    auto  results = sst->get_prefix_range(prefix, tranc_id_);
    for (auto& [key, value, tranc_id] : results) {
      auto it = merged.find(key);
      if (it == merged.end() || it->second.second < tranc_id)
        merged[key] = {value, tranc_id};
    }
  }

  for (size_t level = 1; level <= cur_max_level; ++level) {
    const auto& sst_ids = level_sst_ids[level];
    if (sst_ids.empty()) continue;

    std::string prefix_end = prefix;
    prefix_end.back()++;

    for (auto& sst_id : sst_ids) {
      auto& sst = ssts[sst_id];
      if (sst->get_first_key() >= prefix_end) break;
      auto results = sst->get_prefix_range(prefix, tranc_id_);
      for (auto& [key, value, tranc_id] : results) {
        auto it = merged.find(key);
        if (it == merged.end() || it->second.second < tranc_id)
          merged[key] = {value, tranc_id};
      }
    }
  }

  std::vector<std::tuple<std::string, std::string, uint64_t>> results;
  for (auto& [key, val_tranc] : merged) {
    auto& [value, tranc_id] = val_tranc;
    if (!value.empty())
      results.emplace_back(key, value, tranc_id);
  }
  std::ranges::sort(results, [](const auto& a, const auto& b) {
    if (std::get<0>(a) != std::get<0>(b))
      return std::get<0>(a) < std::get<0>(b);
    return std::get<2>(a) > std::get<2>(b);
  });
  return results;
}

std::vector<std::pair<std::string, std::string>> LSM_Engine::print_level_range(size_t level) {
  std::shared_lock<std::shared_mutex>              lock_(ssts_mtx);
  std::vector<std::pair<std::string, std::string>> result;

  auto it = level_sst_ids.find(level);
  if (it == level_sst_ids.end()) return result;

  for (auto& sst_id : it->second) {
    auto sst_it = ssts.find(sst_id);
    if (sst_it == ssts.end() || !sst_it->second) continue;
    result.emplace_back(sst_it->second->get_first_key(), sst_it->second->get_last_key());
  }
  return result;
}

std::optional<std::pair<std::string, uint64_t>> LSM_Engine::get(std::string_view key,
                                                                 uint64_t        tranc_id) {
  auto mem_res = memtable->get(key, tranc_id);
  if (mem_res.has_value()) {
    if (mem_res.value().first.empty()) return std::nullopt;
    return std::pair<std::string, uint64_t>{mem_res.value().first, mem_res.value().second};
  }

  std::shared_lock<std::shared_mutex> rlock(ssts_mtx);

  for (auto& sst_id : level_sst_ids[0]) {
    auto& sst = ssts[sst_id];
    auto  res = sst->KeyExists(key, tranc_id);
    if (res.has_value()) {
      if (res->first.empty()) return std::nullopt;
      return res;
    }
  }

  for (size_t level = 1; level <= cur_max_level; level++) {
    auto& l_sst_ids = level_sst_ids[level];
    size_t left = 0, right = l_sst_ids.size();
    while (left < right) {
      size_t mid = left + (right - left) / 2;
      auto&  sst = ssts[l_sst_ids[mid]];
      auto   res = sst->KeyExists(key, tranc_id);
      if (res.has_value()) return res;
      else if (sst->get_last_key() < key) left = mid + 1;
      else right = mid;
    }
  }
  return std::nullopt;
}

std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>>
LSM_Engine::get_batch(const std::vector<std::string>& keys, uint64_t tranc_id_) {
  auto memtable_results = memtable->get_batch(keys, tranc_id_);

  std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>> results;
  std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>> un_search;

  for (auto& [key, value, tranc_id] : memtable_results) {
    if (!value.has_value())
      un_search.emplace_back(key, std::string(), tranc_id_);
    else if (value.value().empty())
      results.emplace_back(key, std::nullopt, tranc_id);
    else
      results.emplace_back(key, value, tranc_id);
  }
  if (un_search.empty()) return results;

  std::shared_lock<std::shared_mutex> rlock(ssts_mtx);

  for (auto& sst_id : level_sst_ids[0]) {
    auto& sst = ssts[sst_id];
    for (auto& [k, v, tranc_id] : un_search) {
      if (v.has_value() && v.value().empty()) {
        auto res = sst->get_Iterator(k, tranc_id);
        if (res.valid()) {
          auto val_str   = res->second;
          auto val_tranc = res.get_tranc_id();
          v        = val_str.empty() ? std::nullopt : std::optional<std::string>(val_str);
          tranc_id = val_tranc;
        }
      }
    }
  }

  std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>> un_search_L0;
  for (auto& item : un_search) {
    auto& v = std::get<1>(item);
    if (v.has_value() && v.value().empty())
      un_search_L0.emplace_back(std::get<0>(item), std::string(), tranc_id_);
    else
      results.emplace_back(item);
  }

  for (size_t level = 1; level <= cur_max_level && !un_search_L0.empty(); ++level) {
    const auto& sst_ids = level_sst_ids[level];
    if (sst_ids.empty()) continue;

    std::vector<std::pair<size_t, size_t>> key_to_sst_idx;
    key_to_sst_idx.reserve(un_search_L0.size());

    for (size_t i = 0; i < un_search_L0.size(); ++i) {
      const auto& key = std::get<0>(un_search_L0[i]);
      if (!std::get<1>(un_search_L0[i]).has_value() ||
          !std::get<1>(un_search_L0[i]).value().empty())
        continue;
      size_t left = 0, right = sst_ids.size();
      while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (ssts[sst_ids[mid]]->get_first_key() <= key) left = mid + 1;
        else right = mid;
      }
      if (left == 0) continue;
      size_t idx = left - 1;
      if (key <= ssts[sst_ids[idx]]->get_last_key())
        key_to_sst_idx.emplace_back(i, idx);
    }

    std::sort(key_to_sst_idx.begin(), key_to_sst_idx.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });

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
        if (val.has_value() && val.value().empty()) {
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

  for (auto& [key, val, tranc] : un_search_L0)
    results.emplace_back(key, (val.has_value() && val.value().empty()) ? std::nullopt : val, tranc);

  return results;
}

uint64_t LSM_Engine::bytes_to_mb(size_t bytes) const {
  return bytes / (1024ULL * 1024ULL);
}

// ════════════════════════════════════════════════════════════════════════════
//  Write paths  — WAL logged before memtable
// ════════════════════════════════════════════════════════════════════════════

uint64_t LSM_Engine::put(const std::string& key, const std::string& value, uint64_t tranc_id) {
  // WAL write must succeed before the entry is visible in the memtable.
  // On failure we log the error but do not propagate it upward (matching the
  // existing void-return contract of LSM::put).
  if (auto r = wal->log(WalEntry{key, value, tranc_id}); !r)
    spdlog::error("WAL log failed for key='{}': error {}", key, static_cast<int>(r.error()));

  memtable->put_mutex(key, value, tranc_id);
  if (memtable->get_total_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE)
    compaction_cv_.notify_one();
  return 0;
}

uint64_t LSM_Engine::put_batch(const std::vector<std::pair<std::string, std::string>>& kvs,
                               uint64_t                                                tranc_id) {
  // Build WAL entries and flush the whole batch in a single fsync.
  std::vector<WalEntry> entries;
  entries.reserve(kvs.size());
  for (const auto& [k, v] : kvs)
    entries.push_back({k, v, tranc_id});

  if (auto r = wal->log_batch(std::span{entries}); !r)
    spdlog::error("WAL log_batch failed: error {}", static_cast<int>(r.error()));

  memtable->put_batch(kvs, tranc_id);
  if (memtable->get_total_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE)
    compaction_cv_.notify_one();
  return 0;
}

uint64_t LSM_Engine::remove(const std::string& key, uint64_t tranc_id) {
  // Empty value is the tombstone convention throughout the LSM stack.
  if (auto r = wal->log(WalEntry{key, /*tombstone*/"", tranc_id}); !r)
    spdlog::error("WAL log failed for remove key='{}': error {}", key,
                  static_cast<int>(r.error()));

  memtable->remove(key, tranc_id);
  if (memtable->get_total_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE)
    compaction_cv_.notify_one();
  return 0;
}

uint64_t LSM_Engine::remove_batch(const std::vector<std::string>& keys, uint64_t tranc_id) {
  std::vector<WalEntry> entries;
  entries.reserve(keys.size());
  for (const auto& key : keys)
    entries.push_back({key, /*tombstone*/"", tranc_id});

  if (auto r = wal->log_batch(std::span{entries}); !r)
    spdlog::error("WAL log_batch failed for remove_batch: error {}",
                  static_cast<int>(r.error()));

  memtable->remove_batch(keys, tranc_id);
  if (memtable->get_total_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE)
    compaction_cv_.notify_one();
  return 0;
}

// ════════════════════════════════════════════════════════════════════════════
//  flush — memtable → L0 SST, with MANIFEST + WAL checkpoint update
// ════════════════════════════════════════════════════════════════════════════

uint64_t LSM_Engine::flush(bool force) {
  if (memtable->get_total_size() == 0) return 0;

  const size_t new_sst_id = next_sst_id.fetch_add(1);
  Sstbuild     builder(Global_::Block_SIZE, true);
  const auto   sst_path = get_sst_path(new_sst_id, 0);

  std::unique_lock<std::shared_mutex> lock(ssts_mtx);
  memtable->frozen_cur_table(force);
  auto res = memtable->flushtodisk();
  if (!res) return 0;

  for (auto i = res->begin(); i != res->end(); ++i) {
    auto kv      = i.getValue();
    auto tid     = i.get_tranc_id();
    builder.add(kv.first, kv.second, tid);
  }

  auto new_sst = builder.build(block_cache, sst_path, new_sst_id);

  // ── MANIFEST: persist ADD_SST before updating in-memory index ────────────
  //  If we crash after this write but before the index update the SST exists
  //  on disk and the MANIFEST records it — safe to replay on next startup.
  auto [min_tid, max_tid] = new_sst->get_tranc_id_range();
  manifest_->add_sst(SstMeta{
      .sst_id       = new_sst_id,
      .level        = 0,
      .min_tranc_id = min_tid,
      .max_tranc_id = max_tid,
      .first_key    = new_sst->get_first_key(),
      .last_key     = new_sst->get_last_key(),
  });

  // ── WAL checkpoint: inform WAL that entries up to this point are safe ─────
  //  The WAL cleaner will eventually delete segments whose every tranc_id
  //  is <= checkpoint.
  wal->set_checkpoint_tranc_id(manifest_->checkpoint_tranc_id());

  // ── Update in-memory SST index ────────────────────────────────────────────
  ssts[new_sst_id] = new_sst;
  level_sst_ids[0].push_front(new_sst_id);

  if (level_sst_ids.count(0) && level_sst_ids[0].size() >= Global_::LSM_SST_LEVEL_RATIO)
    full_compact(0);

  return max_tid;
}

// ════════════════════════════════════════════════════════════════════════════
//  clear — wipe all state and reinitialise manifest + WAL from scratch
// ════════════════════════════════════════════════════════════════════════════

void LSM_Engine::clear() {
  level_sst_ids.clear();
  ssts.clear();
  memtable->clear();
  std::fill(level_size.begin(), level_size.end(), 0);
  cur_max_level = 0;

  // Delete all on-disk files (SSTs, WAL segments, MANIFEST).
  try {
    for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
      if (entry.is_regular_file())
        std::filesystem::remove(entry.path());
    }
  } catch (const std::filesystem::filesystem_error& e) {
    spdlog::error("Error clearing directory: {}", e.what());
  }

  // Reinitialise manifest and WAL so subsequent writes work correctly.
  // The old objects' destructors run here (WAL flushes + joins cleaner).
  manifest_ = std::make_unique<Manifest>(data_dir);
  wal       = std::make_unique<WAL>(data_dir, /*checkpoint=*/0);

  next_sst_id.store(0, std::memory_order_relaxed);
  nextTransactionId_.store(1, std::memory_order_relaxed);
}

// ════════════════════════════════════════════════════════════════════════════
//  get_manifest_info
// ════════════════════════════════════════════════════════════════════════════

std::vector<SstMeta> LSM_Engine::get_manifest_info() const {
  return manifest_->get_live_ssts();
}

// ════════════════════════════════════════════════════════════════════════════
//  Compaction helpers
// ════════════════════════════════════════════════════════════════════════════

std::string LSM_Engine::get_sst_path(size_t sst_id, size_t target_level) {
  std::stringstream ss;
  ss << data_dir << "/sst_" << std::setfill('0') << std::setw(32) << sst_id
     << '.' << target_level;
  return ss.str();
}

void LSM_Engine::compaction_worker() {
  while (true) {
    {
      std::unique_lock lk(compaction_mutex_);
      compaction_cv_.wait_for(lk, std::chrono::milliseconds(200), [this] {
        return stop_compaction_.load(std::memory_order_relaxed) ||
               memtable->get_total_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE;
      });
    }
    if (stop_compaction_.load(std::memory_order_relaxed)) break;
    if (memtable->get_total_size() >= Global_::MAX_MEMTABLE_SIZE_PER_TABLE)
      flush();
  }
  if (memtable->get_total_size() > 0) flush();
}

bool LSM_Engine::exit_valid_sst_iter(std::vector<SstIterator>& sst_iters) {
  for (auto& it : sst_iters)
    if (it.valid()) return true;
  return false;
}

std::pair<size_t, size_t> LSM_Engine::find_the_small_kv(std::vector<SstIterator>& sst_iters) {
  std::size_t index{0};
  auto valid_end = std::ranges::remove_if(sst_iters, [](const SstIterator& it) {
    return !it.valid();
  });
  sst_iters.erase(valid_end.begin(), valid_end.end());
  auto res = std::ranges::min_element(sst_iters, [&](const SstIterator& a, const SstIterator& b) {
    if (a.key() != b.key()) return a.key() < b.key();
    index++;
    return a.get_tranc_id() > b.get_tranc_id();
  });
  return std::make_pair(res - sst_iters.begin(), index);
}

// ─── full_compact ──────────────────────────────────────────────────────────
//
//  Crash-safety ordering:
//    1. Write ADD_SST for every new SST to MANIFEST (fsynced).
//       On crash here, old SSTs still exist and MANIFEST lists both — safe.
//    2. Delete each old SST file, then write its REMOVE_SST (fsynced).
//       On crash here, MANIFEST may list an already-deleted SST.
//       Recovery simply skips missing files with a warning.
//    3. Update in-memory index and recurse if the target level overflows.

void LSM_Engine::full_compact(size_t src_level) {
  auto old_level_id_x = level_sst_ids[src_level];
  auto old_level_id_y = level_sst_ids[src_level + 1];

  std::vector<size_t> lx_ids(old_level_id_x.begin(), old_level_id_x.end());
  std::vector<size_t> ly_ids(old_level_id_y.begin(), old_level_id_y.end());

  std::vector<std::shared_ptr<Sstable>> new_ssts =
      (src_level == 0) ? full_l0_l1_compact(lx_ids, ly_ids)
                       : full_common_compact(lx_ids, ly_ids, src_level + 1);

  const size_t target_level = src_level + 1;

  // ── Step 1: ADD_SST for every newly produced SST ──────────────────────────
  for (auto& sst : new_ssts) {
    auto [min_tid, max_tid] = sst->get_tranc_id_range();
    manifest_->add_sst(SstMeta{
        .sst_id       = sst->get_sst_id(),
        .level        = target_level,
        .min_tranc_id = min_tid,
        .max_tranc_id = max_tid,
        .first_key    = sst->get_first_key(),
        .last_key     = sst->get_last_key(),
    });
  }

  // ── Step 2: delete old SSTs + write REMOVE_SST for each ──────────────────
  for (auto old_id : old_level_id_x) {
    level_size[src_level] -= ssts[old_id]->get_sst_size();
    ssts[old_id]->del_sst();
    ssts.erase(old_id);
    manifest_->remove_sst(old_id);
  }
  for (auto old_id : old_level_id_y) {
    level_size[target_level] -= ssts[old_id]->get_sst_size();
    ssts[old_id]->del_sst();
    ssts.erase(old_id);
    manifest_->remove_sst(old_id);
  }

  level_sst_ids[src_level].clear();
  level_sst_ids[target_level].clear();
  cur_max_level = std::max(cur_max_level, target_level);

  // ── Step 3: register new SSTs in the in-memory index ─────────────────────
  for (auto& sst : new_ssts) {
    const size_t id = sst->get_sst_id();
    level_size[target_level] += sst->get_sst_size();
    level_sst_ids[target_level].push_back(id);
    ssts[id] = sst;
  }
  std::sort(level_sst_ids[target_level].begin(), level_sst_ids[target_level].end());

  // Recurse if the target level is now over its size budget.
  if (bytes_to_mb(level_size[target_level]) >= 10 * std::pow(10, src_level))
    full_compact(target_level);
}

// ─── compact helpers (unchanged) ──────────────────────────────────────────

std::vector<std::shared_ptr<Sstable>> LSM_Engine::full_l0_l1_compact(
    std::vector<size_t>& l0_ids, std::vector<size_t>& l1_ids) {
  std::vector<std::shared_ptr<Sstable>> sst;
  sst.reserve(std::max(l0_ids.size(), l1_ids.size()) + 1);
  auto merged  = merge_sst_iterator(l0_ids, l1_ids);
  auto builder = std::make_unique<Sstbuild>(Global_::Block_CACHE_capacity);

  auto advance_all_with_key = [&](const std::string& key) {
    for (auto& it : merged)
      while (it.valid() && it.key() == key) ++it;
  };
  auto advance_one = [&](size_t idx, const std::string& key) {
    while (merged[idx].valid() && merged[idx].key() == key) ++merged[idx];
  };
  auto find_best = [&]() -> std::pair<size_t, bool> {
    std::string min_key;
    uint64_t    max_tranc = 0;
    size_t      best_idx  = SIZE_MAX;
    bool        has_dup   = false;
    for (size_t i = 0; i < merged.size(); ++i) {
      if (!merged[i].valid()) continue;
      const auto& k = merged[i].key();
      if (best_idx == SIZE_MAX || k < min_key) {
        min_key = k; max_tranc = merged[i].get_tranc_id();
        best_idx = i; has_dup = false;
      } else if (k == min_key) {
        has_dup = true;
        if (merged[i].get_tranc_id() > max_tranc) {
          max_tranc = merged[i].get_tranc_id(); best_idx = i;
        }
      }
    }
    return {best_idx, has_dup};
  };

  while (exit_valid_sst_iter(merged)) {
    auto [best_idx, has_dup] = find_best();
    if (best_idx == SIZE_MAX) break;
    std::string cur_key = merged[best_idx].key();
    builder->add(cur_key, merged[best_idx].value(), merged[best_idx].get_tranc_id());
    if (has_dup) advance_all_with_key(cur_key);
    else          advance_one(best_idx, cur_key);
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

std::vector<std::shared_ptr<Sstable>> LSM_Engine::full_common_compact(
    std::vector<size_t>& lx_ids, std::vector<size_t>& ly_ids, size_t level_y) {
  std::vector<std::shared_ptr<Sstable>> sst;
  sst.reserve(std::max(lx_ids.size(), ly_ids.size()) + 1);
  auto merged  = merge_sst_iterator(lx_ids, ly_ids);
  auto builder = std::make_unique<Sstbuild>(Global_::Block_CACHE_capacity);

  auto advance_all_with_key = [&](const std::string& key) {
    for (auto& it : merged)
      while (it.valid() && it.key() == key) ++it;
  };
  auto advance_one = [&](size_t idx, const std::string& key) {
    while (merged[idx].valid() && merged[idx].key() == key) ++merged[idx];
  };
  auto find_best = [&]() -> std::pair<size_t, bool> {
    std::string min_key;
    uint64_t    max_tranc = 0;
    size_t      best_idx  = SIZE_MAX;
    bool        has_dup   = false;
    for (size_t i = 0; i < merged.size(); ++i) {
      if (!merged[i].valid()) continue;
      const auto& k = merged[i].key();
      if (best_idx == SIZE_MAX || k < min_key) {
        min_key = k; max_tranc = merged[i].get_tranc_id();
        best_idx = i; has_dup = false;
      } else if (k == min_key) {
        has_dup = true;
        if (merged[i].get_tranc_id() > max_tranc) {
          max_tranc = merged[i].get_tranc_id(); best_idx = i;
        }
      }
    }
    return {best_idx, has_dup};
  };

  while (exit_valid_sst_iter(merged)) {
    auto [best_idx, has_dup] = find_best();
    if (best_idx == SIZE_MAX) break;
    std::string cur_key = merged[best_idx].key();
    builder->add(cur_key, merged[best_idx].value(), merged[best_idx].get_tranc_id());
    if (has_dup) advance_all_with_key(cur_key);
    else          advance_one(best_idx, cur_key);
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

std::vector<std::shared_ptr<Sstable>> LSM_Engine::gen_sst_from_iter(
    BaseIterator& iter, size_t target_sst_size, size_t target_level) {
  std::vector<std::shared_ptr<Sstable>> new_ssts;
  auto new_sst_builder = Sstbuild(Global_::Block_SIZE, true);
  while (iter.valid() && !iter.isEnd()) {
    new_sst_builder.add((*iter).first, (*iter).second, 0);
    ++iter;
    if (new_sst_builder.estimated_size() >= target_sst_size) {
      size_t sst_id = next_sst_id++;
      new_ssts.push_back(new_sst_builder.build(block_cache, get_sst_path(sst_id, target_level), sst_id));
      new_sst_builder = Sstbuild(Global_::Block_SIZE, true);
    }
  }
  if (new_sst_builder.estimated_size() > 0) {
    size_t sst_id = next_sst_id++;
    new_ssts.push_back(new_sst_builder.build(block_cache, get_sst_path(sst_id, target_level), sst_id));
  }
  return new_ssts;
}

size_t LSM_Engine::get_sst_size(size_t level) {
  if (level == 0) return Global_::MAX_MEMTABLE_SIZE_PER_TABLE;
  return Global_::MAX_MEMTABLE_SIZE_PER_TABLE *
         static_cast<size_t>(std::pow(Global_::LSM_SST_LEVEL_RATIO, level));
}

std::vector<SstIterator> LSM_Engine::merge_sst_iterator(std::vector<std::size_t> iter_id0,
                                                        std::vector<std::size_t> iter_id1) {
  if (iter_id0.empty() && iter_id1.empty()) return {};
  std::vector<SstIterator> l0_l1_iters;
  l0_l1_iters.reserve(iter_id0.size() + iter_id1.size());
  for (auto id : iter_id0) l0_l1_iters.push_back(ssts[id]->begin(0));
  for (auto id : iter_id1) l0_l1_iters.push_back(ssts[id]->begin(0));
  return l0_l1_iters;
}

// ════════════════════════════════════════════════════════════════════════════
//  LSM façade
// ════════════════════════════════════════════════════════════════════════════

LSM::LSM(std::string path) : engine(std::make_shared<LSM_Engine>(path)) {}

LSM::~LSM() { flush_all(); }

uint64_t LSM::getNextTransactionId() {
  return engine->nextTransactionId_.fetch_add(1);
}

void LSM::print_level_range(size_t level) {
  for (auto& [key, value] : engine->print_level_range(level))
    std::print("key:{},value:{}\n", key, value);
}

std::vector<SstMeta> LSM::get_manifest_info() const {
  return engine->get_manifest_info();
}

std::optional<std::string> LSM::get(std::string_view key) {
  auto res = engine->get(key, getNextTransactionId());
  if (res.has_value()) return res.value().first;
  return std::nullopt;
}

std::vector<std::pair<std::string, std::optional<std::string>>> LSM::get_batch(
    const std::vector<std::string>& keys) {
  auto batch_results = engine->get_batch(keys, getNextTransactionId());
  std::vector<std::pair<std::string, std::optional<std::string>>> results;
  for (const auto& [key, value, tr] : batch_results)
    results.emplace_back(key, value);
  return results;
}

std::vector<std::pair<std::string, std::string>> LSM::range(const std::string& /*start_key*/,
                                                             const std::string& /*end_key*/) {
  // TODO: implement via Level_Iterator
  return {};
}

std::vector<std::tuple<std::string, std::string, uint64_t>> LSM::get_prefix_range(
    const std::string& prefix) {
  return engine->get_prefix_range(prefix, getNextTransactionId());
}

void LSM::put(const std::string& key, const std::string& value) {
  engine->put(key, value, getNextTransactionId());
}

void LSM::put_batch(const std::vector<std::pair<std::string, std::string>>& kvs) {
  engine->put_batch(kvs, getNextTransactionId());
}

void LSM::remove(const std::string& key) {
  engine->remove(key, getNextTransactionId());
}

void LSM::remove_batch(const std::vector<std::string>& keys) {
  engine->remove_batch(keys, getNextTransactionId());
}

void LSM::clear() { engine->clear(); }

void LSM::flush(bool force) { engine->flush(force); }

void LSM::flush_all() {
  while (engine->memtable->get_total_size() > 0)
    engine->flush(true);
}