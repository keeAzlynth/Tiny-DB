#include "../../include/memtable.h"
#include "../../include/Blockcache.h"
#include "../../include/SstableIterator.h"
#include "../../include/Sstable.h"
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <cmath>
#include <cstddef>
#include <memory>
#include <filesystem>
#include <string>
#include <tuple>
#include <algorithm>
#include <utility>
#include <vector>

class SstableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 初始化 BlockCache
    block_cache = std::make_shared<BlockCache>(4096, 2);
    memtable    = std::make_unique<MemTable>();
    if (std::filesystem::exists(tmp_path1)&& std::filesystem::exists(tmp_path2)) {
    }
    else {
      std::filesystem::create_directory(tmp_path1);
      std::filesystem::create_directory(tmp_path2);
    }
  }

  void TearDown() override {
    try {
      if (!tmp_path1.empty() && std::filesystem::exists(tmp_path1)) {
        std::filesystem::remove(tmp_path1);
      }
      if (!tmp_path2.empty() && std::filesystem::exists(tmp_path2)) {
        std::filesystem::remove(tmp_path2);
      }
    } catch (...) {
    }
  }
  std::vector<std::tuple<std::string, std::string, uint64_t>> generate_range_test_data(
      int num_records, size_t version) {
    std::vector<std::tuple<std::string, std::string, uint64_t>> data;

    // 生成连续的范围数据，如 key_0000, key_0001, key_0002...
    for (int i = 0; i < num_records; ++i) {
      std::string key = std::format("key_{:04d}", i);

      // 每个键有3个版本
      data.emplace_back(key, std::format("value_v1_{:04d}", i), 1000 + i);
      data.emplace_back(key, std::format("value_v2_{:04d}", i), 2000 + i);
      data.emplace_back(key, std::format("value_v3_{:04d}", i), 3000 + i);
    }

    // 按时间戳排序（模拟写入顺序）
    std::ranges::sort(data,
                      [](const auto& a, const auto& b) { return std::get<2>(a) < std::get<2>(b); });

    return data;
  }

  // 构建SSTable
  std::shared_ptr<Sstable> build_sstable(
      const std::vector<std::tuple<std::string, std::string, uint64_t>>& data,
      const std::string& path, size_t block_size = 4096) {
if (std::filesystem::exists(path)) {
      std::filesystem::remove(path);
    }
    Sstbuild builder(block_size, true);

    // 将所有数据放入memtable
    for (const auto& [key, value, ts] : data) {
      memtable->put(key, value, ts);
    }

    // 刷入builder
    auto flush_result = memtable->flush();
    if (!flush_result) {
      spdlog::error("Flush failed");
    }

    for (auto it = flush_result->begin(); it != flush_result->end(); ++it) {
      auto kv       = it.getValue();
      auto tranc_id = it.get_tranc_id();
      builder.add(kv.first, kv.second, tranc_id);
    }

    return builder.build(block_cache, path, 0);
  }

  std::unique_ptr<MemTable>   memtable;
  std::shared_ptr<BlockCache> block_cache;
  std::string tmp_path1 = "../../../lsm/lsm_test_sstable.dat";  // 改成你的绝对路径，方便调试
  std::string tmp_path2 = "../../../lsm/lsm_test_sstable2.dat";
};

// 使用大量数据并强制产生多个 block，验证前缀查询和返回结果正确性
TEST_F(SstableTest, BuildAndGetPrefixSingleKey_ManyBlocks) {
  const std::string key_prefix  = "k";
  const size_t      num_records = 500;   // 较大数据量
  const size_t      block_size  = 4096;  // 较小 block 容量以产生多个 block

  if (std::filesystem::exists(tmp_path1))
    std::filesystem::remove(tmp_path1);

  Sstbuild                                         builder(block_size, true);
  std::vector<std::pair<std::string, std::string>> kvs;
  kvs.reserve(num_records);
  for (size_t i = 0; i < num_records; ++i) {
    auto res = Global_::generateRandom(0, num_records - 1);
    kvs.push_back({key_prefix + std::to_string(res), "value" + std::to_string(res)});
  }
  // 先将所有数据 put 到 memtable
  for (auto& i : kvs) {
    memtable->put(i.first, i.second, 0);
  }
  // 然后在最后调用一次 flushsync，把所有数据刷到 builder
  auto res = memtable->flush();
  for (auto i = res->begin(); i != res->end(); ++i) {
    auto kv = i.getValue();
    builder.add(kv.first, kv.second);
  }

  std::shared_ptr<Sstable> sst;
  try {
    sst = builder.build(block_cache, tmp_path1, 0);
  } catch (const std::exception& e) {
    FAIL() << "Build failed: " << e.what();
    return;
  }

  ASSERT_NE(sst, nullptr);
  ASSERT_TRUE(std::filesystem::exists(tmp_path1));

  // 选取若干前缀检查，确保跨 block 能正确查询
  auto                                                        checks_size = 10;
  std::vector<std::tuple<std::string, std::string, uint64_t>> checks;
  checks.reserve(checks_size);
  for (int i = 0; i < checks_size; i++) {
    auto index = Global_::generateRandom(0, num_records - 1);
    checks.push_back({kvs[index].first, kvs[index].second, 0});
  }

  for (const auto& pref : checks) {
    auto results = sst->get_prefix_range(std::get<0>(pref), 0);
    ASSERT_FALSE(results.empty()) << "Prefix query returned empty for " << std::get<0>(pref);

    // 检查每个结果确实以 pref 开头
    for (const auto& p : results) {
      ASSERT_TRUE(std::get<0>(p).starts_with(std::get<0>(pref)))
          << "Key '" << std::get<0>(p) << "' does not start with prefix '" << std::get<0>(pref)
          << "'";
    }
  }

  // 验证某些具体键存在且值正确
  auto r     = sst->get_prefix_range("k12", 0);
  bool found = false;
  for (auto& p : r) {
    if (std::get<0>(p).starts_with("k12")) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}

// 读取多个 block 并验证缓存命中/不同 block 返回不同对象
TEST_F(SstableTest, ReadMultipleBlocksAndCache) {
  const std::string key_prefix  = "bk";
  const size_t      num_records = 2000;
  const size_t      block_size  = 256;

  if (std::filesystem::exists(tmp_path2))
    std::filesystem::remove(tmp_path2);

  Sstbuild builder(block_size, false);  // 关闭 bloom 以简化
  for (size_t i = 0; i < num_records; ++i) {
    builder.add(key_prefix + std::to_string(i), "v" + std::to_string(i), 0);
  }

  std::shared_ptr<Sstable> sst;
  try {
    sst = builder.build(block_cache, tmp_path2, 2);
  } catch (const std::exception& e) {
    FAIL() << "Build failed: " << e.what();
    return;
  }

  ASSERT_NE(sst, nullptr);

  // 尝试读取前 5 个 block（如果存在），并验证缓存行为
  std::vector<std::shared_ptr<Block>> first_reads;
  for (size_t idx = 0; idx < 5; ++idx) {
    try {
      auto b = sst->read_block(idx);
      if (b == nullptr)
        break;
      first_reads.push_back(b);
    } catch (...) {
      break;
    }
  }

  ASSERT_GE(first_reads.size(), 2u) << "预计至少有 2 个 block 被生成，当前不足";

  // 再次读取这些 block，应返回相同的 shared_ptr（缓存命中）
  for (size_t i = 0; i < first_reads.size(); ++i) {
    auto b2 = sst->read_block(i);
    ASSERT_NE(b2, nullptr);
    EXPECT_EQ(first_reads[i], b2) << "Cache miss or different object for block " << i;
  }

  // 验证不同 block 返回不同对象指针
  if (first_reads.size() >= 2) {
    EXPECT_NE(first_reads[0], first_reads[1]);
  }
}

// Collect keys from [begin, end) and verify prefix
static std::vector<std::string> collect_prefix_keys(const std::shared_ptr<BlockIterator>& begin,
                                                    const std::shared_ptr<BlockIterator>& end,
                                                    const std::string&                    prefix) {
  std::vector<std::string> out;
  if (!begin || !end)
    return out;
  // iterate by index comparison (end is exclusive)
  while (begin->getIndex() != end->getIndex()) {
    auto kv = begin->getValue();
    // stop if not matching prefix
    if (!kv.first.starts_with(prefix))
      break;
    out.push_back(kv.first);
    ++(*begin);
  }
  return out;
}

// 测试MVCC点查询功能
TEST_F(SstableTest, MvccPointQuery) {
  auto test_data = generate_range_test_data(1000, 3);  // 生成1000个key确保足够的数据
  auto sst       = build_sstable(test_data, tmp_path1);

  ASSERT_NE(sst, nullptr);

  // 测试1：key_0050 在不同时间戳的可见版本
  {
    auto results_1050 = sst->get_prefix_range("key_0050", 1050);
    ASSERT_EQ(results_1050.size(), 1u) << "At ts=1050, should see 1 version";
    EXPECT_EQ(std::get<0>(results_1050[0]), "key_0050");
    EXPECT_EQ(std::get<2>(results_1050[0]), 1050u);

    auto results_2050 = sst->get_prefix_range("key_0050", 2050);
    EXPECT_GE(results_2050.size(), 2u) << "At ts=2050, should see at least 2 versions (1050, 2050)";

    auto results_3050 = sst->get_prefix_range("key_0050", 3050);
    EXPECT_GE(results_3050.size(), 3u)
        << "At ts=3050, should see at least 3 versions (1050, 2050, 3050)";
  }

  // 测试2：时间戳之前没有数据
  {
    auto results = sst->get_prefix_range("key_0050", 999);
    EXPECT_TRUE(results.empty()) << "Before ts=1000, should have no data";
  }

  // 测试3：验证所有结果都匹配精确的key
  {
    auto results = sst->get_prefix_range("key_0500", 2500);
    for (const auto& [k, v, ts] : results) {
      EXPECT_EQ(k, "key_0500");
      EXPECT_LE(ts, 2500u);
    }
    EXPECT_FALSE(results.empty()) << "Should find key_0500 versions";
  }
}

// 测试MVCC范围查询
TEST_F(SstableTest, MvccRangeQuery) {
  auto test_data = generate_range_test_data(1000, 3);  // 生成1000个key
  auto sst       = build_sstable(test_data, tmp_path2);

  ASSERT_NE(sst, nullptr);

  // 测试1：前缀查询 "key_01" 应该返回 key_0100 到 key_0199（共100个key），每个可能有多个版本
  {
    auto results = sst->get_prefix_range("key_01", 1500);
    EXPECT_GT(results.size(), 0u) << "Prefix 'key_01' should return results at ts=1500";

    // 验证所有结果都以 "key_01" 开头
    for (const auto& [key, value, ts] : results) {
      EXPECT_TRUE(key.starts_with("key_01")) << "Key " << key << " should start with 'key_01'";
      EXPECT_LE(ts, 1500u) << "Timestamp should be <= 1500";
    }
  }

  // 测试2：前缀查询 "key_00" 在不同时间戳
  {
    auto results_1500 = sst->get_prefix_range("key_00", 1500);
    auto results_2500 = sst->get_prefix_range("key_00", 2500);
    auto results_3500 = sst->get_prefix_range("key_00", 3500);

    // 时间戳越晚，能看到的版本越多
    EXPECT_GT(results_1500.size(), 0u) << "Should find key_00x at ts=1500";
    EXPECT_GE(results_2500.size(), results_1500.size())
        << "Should see more or equal versions at ts=2500 vs ts=1500";
    EXPECT_GE(results_3500.size(), results_2500.size())
        << "Should see more or equal versions at ts=3500 vs ts=2500";
  }
}

// 测试MVCC范围查询包含结束边界
TEST_F(SstableTest, MvccRangeQueryInclusive) {
  auto test_data = generate_range_test_data(1000, 3);
  auto sst       = build_sstable(test_data, tmp_path2);

  ASSERT_NE(sst, nullptr);

  // 测试包含特定边界键的范围查询
  auto range_tests =
      std::to_array<std::tuple<std::string, std::string, uint64_t, std::vector<std::string>>>({
          {"key_0050",
           "key_0060",
           1500,
           {"key_0050", "key_0051", "key_0052", "key_0053", "key_0054", "key_0055", "key_0056",
            "key_0057", "key_0058", "key_0059"}},
          {"key_0998", "key_0999", 2500, {"key_0098", "key_0098", "key_0999", "key_0999"}},
      });

  for (const auto& [start_key, end_key, timestamp, expected_keys] : range_tests) {
    std::vector<std::string> found_keys;

    // 遍历范围内的键
    size_t start_num = 0, end_num = 0;
    if (start_key.starts_with("key_")) {
      try {
        start_num = std::stoi(start_key.substr(4));
      } catch (...) {
      }
    }
    if (end_key.starts_with("key_")) {
      try {
        end_num = std::stoi(end_key.substr(4));
      } catch (...) {
      }
    }

    for (size_t i = start_num; i < end_num; i++) {
      std::string key     = std::format("key_{:04d}", i);
      auto        results = sst->get_Iterator(key, timestamp);
      while (results.isEnd()) {
        int  count = 0;
        auto value = results.getValue();
        if (std::get<0>(value) >= end_key) {
          EXPECT_EQ(count, expected_keys.size());
          break;
        }
        count++;
      }
    }
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}