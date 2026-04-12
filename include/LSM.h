#pragma once
#include "core/Global.h"
#include "iterator/SstableIterator.h"
#include "core/memtable.h"
#include "compaction/Manifest.h"      // ← new: SstMeta + Manifest
#include "storage/Sstable.h"
#include "iterator/TmergeIterator.h"
#include "storage/wal.h"
#include "transaction/transaction.h"
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <print>

class Level_Iterator;

class LSM_Engine : public std::enable_shared_from_this<LSM_Engine> {
 public:
  std::string                                          data_dir;
  std::shared_ptr<MemTable>                            memtable;
  std::map<size_t, std::deque<size_t>>                 level_sst_ids;
  std::unordered_map<size_t, std::shared_ptr<Sstable>> ssts;
  std::array<std::size_t, Global_::MAX_LEVEL>          level_size;
  std::shared_mutex                                    ssts_mtx;
  std::shared_ptr<BlockCache>                          block_cache;
  std::unique_ptr<WAL>                                 wal;
  std::atomic<uint64_t>                                nextTransactionId_ = 1;
  std::atomic_size_t                                   next_sst_id        = 0;
  size_t                                               cur_max_level      = 0;

 public:
  LSM_Engine(std::string path, size_t block_cache_capacity = Global_::Block_CACHE_capacity,
             size_t block_cache_k = Global_::Block_CACHE_K);
  ~LSM_Engine();

  std::vector<std::tuple<std::string, std::string, uint64_t>> get_prefix_range(
      const std::string& prefix, uint64_t tranc_id);
  std::vector<std::pair<std::string, std::string>>     print_level_range(size_t level);
  std::optional<std::pair<std::string, uint64_t>> get(std::string_view key,
                                                           uint64_t         tranc_id = 0);
  std::vector<std::tuple<std::string, std::optional<std::string>, uint64_t>> get_batch(
      const std::vector<std::string>& keys, uint64_t tranc_id = 0);
  uint64_t bytes_to_mb(size_t bytes) const;

  // Returns a snapshot of all live SST metadata recorded in the MANIFEST.
  [[nodiscard]] std::vector<SstMeta> get_manifest_info() const;

  // 如果触发了刷盘, 返回当前刷入sst的最大事务id
  uint64_t put(const std::string& key, const std::string& value, uint64_t tranc_id = 0);
  uint64_t put_batch(const std::vector<std::pair<std::string, std::string>>& kvs,
                     uint64_t                                                tranc_id = 0);
  uint64_t remove(const std::string& key, uint64_t tranc_id = 0);
  uint64_t remove_batch(const std::vector<std::string>& keys, uint64_t tranc_id = 0);
  void     clear();
  uint64_t flush(bool force = false);

  std::string get_sst_path(size_t sst_id, size_t target_level);
  static size_t            get_sst_size(size_t level);
  std::vector<SstIterator> merge_sst_iterator(std::vector<std::size_t> iter_id0,
                                              std::vector<std::size_t> iter_id1);

 private:
  // ── MANIFEST ──────────────────────────────────────────────────────────────
  std::unique_ptr<Manifest> manifest_;

  // ── Compaction ────────────────────────────────────────────────────────────
  std::thread             compaction_thread_;
  std::mutex              compaction_mutex_;
  std::condition_variable compaction_cv_;
  std::atomic<bool>       stop_compaction_{false};

  void compaction_worker();
  bool exit_valid_sst_iter(std::vector<SstIterator>& sst_iters);
  std::pair<size_t, size_t> find_the_small_kv(std::vector<SstIterator>& sst_iters);
  void full_compact(size_t src_level);
  std::vector<std::shared_ptr<Sstable>> full_l0_l1_compact(std::vector<size_t>& l0_ids,
                                                           std::vector<size_t>& l1_ids);
  std::vector<std::shared_ptr<Sstable>> full_common_compact(std::vector<size_t>& lx_ids,
                                                            std::vector<size_t>& ly_ids,
                                                            size_t               level_y);
  std::vector<std::shared_ptr<Sstable>> gen_sst_from_iter(BaseIterator& iter,
                                                          size_t        target_sst_size,
                                                          size_t        target_level);
};

// ─── Public LSM façade ────────────────────────────────────────────────────────
class LSM {
 private:
  std::shared_ptr<LSM_Engine> engine;
  uint64_t getNextTransactionId();

 public:
  explicit LSM(std::string path);
  ~LSM();

  void print_level_range(size_t level);

  // Returns a snapshot of all live SST metadata from the MANIFEST.
  // Entries are ordered by sst_id (ascending).  Useful for inspecting
  // the current level layout without reading SST files from disk.
  [[nodiscard]] std::vector<SstMeta> get_manifest_info() const;

  std::optional<std::string>                                 get(std::string_view key);
  std::vector<std::pair<std::string, std::optional<std::string>>> get_batch(
      const std::vector<std::string>& keys);
  std::vector<std::pair<std::string, std::string>> range(const std::string& start_key,
                                                         const std::string& end_key);
  std::vector<std::tuple<std::string, std::string, uint64_t>> get_prefix_range(
      const std::string& prefix);

  void put(const std::string& key, const std::string& value);
  void put_batch(const std::vector<std::pair<std::string, std::string>>& kvs);
  void remove(const std::string& key);
  void remove_batch(const std::vector<std::string>& keys);

  using LSMIterator = Level_Iterator;
  void clear();
  void flush(bool force = false);
  void flush_all();
};