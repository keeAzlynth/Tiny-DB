#pragma once
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <tuple>
#include <vector>
#include <string>
#include <variant>
#include "Blockcache.h"
#include "BlockMeta.h"
#include "BloomFilter.h"
#include "file.h"

class SstIterator;
class Sstable : public std::enable_shared_from_this<Sstable> {
  friend class Sstbuild;

 public:
  Sstable() = default;
  void                            del_sst();
  static std::shared_ptr<Sstable> open(size_t sst_id, FileObj file_obj_,
                                       std::shared_ptr<BlockCache> block_cache);

  std::shared_ptr<Sstable>            create_sst_with_meta_only(size_t sst_id, size_t file_size,
                                                                const std::string&          first_key,
                                                                const std::string&          last_key,
                                                                std::shared_ptr<BlockCache> block_cache);
  std::shared_ptr<Block>              read_block(size_t block_idx);
  std::optional<size_t>               find_block_idx(std::string_view key, bool is_prefix = false);
  std::vector<std::shared_ptr<Block>> find_block_range(std::string_view key_prefix);
  size_t                              num_blocks() const;
  size_t                              get_sst_size() const;
  size_t                              get_sst_id() const;
  std::string                         get_first_key() const;
  std::string                         get_last_key() const;
  std::tuple<std::string, std::string, uint64_t> getValue() const;
  bool                                           is_block_index_vaild(size_t block_index) const;
  std::optional<std::pair<std::string, uint64_t>> KeyExists(std::string_view key, uint64_t);
  SstIterator get_Iterator(std::string_view key, uint64_t tranc_id = 0, bool is_prefix = false);
  SstIterator current_Iterator(size_t block_idx, uint64_t tranc_id = 0);
  SstIterator begin(uint64_t tranc_id);
  SstIterator end();
  std::pair<uint64_t, uint64_t>                               get_tranc_id_range() const;
  std::vector<std::tuple<std::string, std::string, uint64_t>> get_prefix_range(std::string_view key,
                                                                               uint64_t tranc_id);

  std::vector<BlockMeta> block_metas;
  uint64_t               min_tranc_id;
  uint64_t               max_tranc_id;

 private:
  FileObj  file_obj;
  uint32_t bloom_offset;
  uint32_t meta_block_offset;
  uint32_t block_offset;

  std::string first_key;
  std::string last_key;

  size_t                       sst_id;
  std::shared_ptr<BloomFilter> bloom_filter;
  std::shared_ptr<BlockCache>  block_cache;
};

class Sstbuild {
 public:
  Sstbuild(size_t block_size, bool has_bloom = true);
  void   clean();
  void   add(const std::string& key, const std::string& value, uint64_t tranc_id = 0);
  void   finish_block();
  size_t estimated_size() const;
  std::shared_ptr<Sstable> build(std::shared_ptr<BlockCache> block_cache,
                                 const std::string& sstable_path, size_t sstid);

 private:
  std::string                  first_key;
  std::string                  last_key;
  uint64_t                     min_tranc_id;
  uint64_t                     max_tranc_id;
  std::shared_ptr<BloomFilter> bloom_filter;
  Block                        block_;
  std::vector<BlockMeta>       block_metas;
  std::vector<uint8_t>         data;
  size_t                       block_size;
};