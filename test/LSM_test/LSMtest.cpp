#include "../../include/LSM.h"
#include "../../include/LeveIterator.h"
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <memory>
#include <optional>
#include <print>
#include <string>
#include <vector>
#include <algorithm>

class LSMTest : public ::testing::Test {
 protected:
  std::string db_path = "../../../lsm/lsm_engine_test";

  void SetUp() override {
    // 用测试名作为路径，完全隔离
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    db_path = std::format("../../../lsm/test_{}_{}", info->test_suite_name(), info->name());

    if (std::filesystem::exists(db_path)) {
      std::filesystem::remove_all(db_path);
    }
    std::filesystem::create_directories(db_path);
    lsm = std::make_shared<LSM>(db_path);
  }

  void TearDown() override { lsm.reset(); }

  std::shared_ptr<LSM> lsm;
};

// ─────────────────────────────────────────────
//  1. 基本读写与删除
// ─────────────────────────────────────────────
/*
// 写入单条记录后能正确读回
TEST_F(LSMTest, PutAndGet_Basic) {
lsm->put("hello", "world");
    auto val = lsm->get("hello");
    ASSERT_TRUE(val.has_value()) << "Key 'hello' should exist";
    EXPECT_EQ(*val, "world");

}

// 覆盖写入后读到最新值
TEST_F(LSMTest, PutOverwrite_ReturnsLatestValue) {
  lsm->put("key1", "v1");
  lsm->put("key1", "v2");
  lsm->put("key1", "v3");

  auto val = lsm->get("key1");
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, "v3") << "Should return the latest written value";
}

// 查询不存在的 key 应返回 nullopt
TEST_F(LSMTest, Get_NonExistentKey_ReturnsNullopt) {
  auto val = lsm->get("no_such_key");
  EXPECT_FALSE(val.has_value()) << "Non-existent key should return nullopt";
}

// 删除后 key 应不可见
TEST_F(LSMTest, Remove_KeyBecomesInvisible) {
  lsm->put("del_key", "some_value");
  lsm->remove("del_key");

  auto val = lsm->get("del_key");
  EXPECT_FALSE(val.has_value()) << "Deleted key should not be visible";
}

// 删除一个从未写入的 key 不应崩溃
TEST_F(LSMTest, Remove_NonExistentKey_NoCrash) {
  EXPECT_NO_THROW(lsm->remove("ghost_key"));
  EXPECT_FALSE(lsm->get("ghost_key").has_value());
}

// ─────────────────────────────────────────────
//  2. 批量读写与删除
// ─────────────────────────────────────────────

// 批量写入后能逐条正确读回
TEST_F(LSMTest, PutBatch_AllKeysReadable) {
  const int N = 200;
  std::vector<std::pair<std::string, std::string>> kvs;
  kvs.reserve(N);
  for (int i = 0; i < N; ++i) {
    kvs.push_back({std::format("batch_key_{:04d}", i),
                   std::format("batch_val_{:04d}", i)});
  }
  lsm->put_batch(kvs);

  for (int i = 0; i < N; ++i) {
    auto val = lsm->get(std::format("batch_key_{:04d}", i));
    ASSERT_TRUE(val.has_value()) << "batch_key_" << i << " should exist";
    EXPECT_EQ(*val, std::format("batch_val_{:04d}", i));
  }
}

// get_batch 混合存在/不存在 key，结果顺序与输入一致
TEST_F(LSMTest, GetBatch_MixedExistenceReturnsCorrectOrder) {
  lsm->put("exists_a", "alpha");
  lsm->put("exists_b", "beta");

  std::vector<std::string> query_keys = {"exists_a", "missing_x", "exists_b", "missing_y"};
  auto results = lsm->get_batch(query_keys);

  ASSERT_EQ(results.size(), query_keys.size());
  EXPECT_EQ(results[0].first, "exists_a");
  EXPECT_TRUE(results[0].second.has_value());
  EXPECT_EQ(*results[0].second, "alpha");

  EXPECT_EQ(results[1].first, "missing_x");
  EXPECT_FALSE(results[1].second.has_value());

  EXPECT_EQ(results[2].first, "exists_b");
  EXPECT_TRUE(results[2].second.has_value());
  EXPECT_EQ(*results[2].second, "beta");

  EXPECT_EQ(results[3].first, "missing_y");
  EXPECT_FALSE(results[3].second.has_value());
}

// 批量删除后所有 key 应不可见
TEST_F(LSMTest, RemoveBatch_AllKeysInvisible) {
  const int N = 50;
  std::vector<std::pair<std::string, std::string>> kvs;
  std::vector<std::string> keys;
  for (int i = 0; i < N; ++i) {
    std::string k = std::format("rm_key_{:04d}", i);
    kvs.push_back({k, "v"});
    keys.push_back(k);
  }
  lsm->put_batch(kvs);
  lsm->remove_batch(keys);

  for (const auto& k : keys) {
    EXPECT_FALSE(lsm->get(k).has_value()) << k << " should be deleted";
  }
}
// ─────────────────────────────────────────────
//  3. 刷盘（flush）与持久化
// ─────────────────────────────────────────────

// flush 后数据仍然可读（memtable → SSTable 路径可查到）
TEST_F(LSMTest, FlushThenGet_DataStillAccessible) {
  const int N = 300;
  for (int i = 0; i < N; ++i) {
    lsm->put(std::format("fkey_{:04d}", i), std::format("fval_{:04d}", i));
  }
  lsm->flush();

  for (int i = 0; i < N; ++i) {
    auto val = lsm->get(std::format("fkey_{:04d}", i));
    ASSERT_TRUE(val.has_value()) << "fkey_" << i << " should survive flush";
    EXPECT_EQ(*val, std::format("fval_{:04d}", i));
  }
}

// flush 后删除标记依然有效
TEST_F(LSMTest, FlushPreservesTombstone) {
  lsm->put("zombie", "alive");
  lsm->flush();
  lsm->remove("zombie");
  lsm->flush();

  EXPECT_FALSE(lsm->get("zombie").has_value()) << "Tombstone should survive double flush";
}




// ─────────────────────────────────────────────
//  4. 大数据量 / 强制多层压缩路径
// ─────────────────────────────────────────────

// 写入足够多的数据以触发 L0→L1 压缩，验证数据完整性
TEST_F(LSMTest, LargeWriteLoad_DataIntegrityAfterCompaction) {
  const int N = 5000;
  for (int i = 0; i < N; ++i) {
    lsm->put(std::format("ckey_{:06d}", i), std::format("cval_{:06d}", i));
    // 每隔 500 条刷一次，制造多个 SSTable 并触发压缩
    if ((i + 1) % 500 == 0) {
      lsm->flush();
    }
  }
  lsm->flush_all();

  // 抽样验证 1/10 的数据
  for (int i = 0; i < N; i += 10) {
    auto val = lsm->get(std::format("ckey_{:06d}", i));
    ASSERT_TRUE(val.has_value()) << "ckey_" << i << " missing after compaction";
    EXPECT_EQ(*val, std::format("cval_{:06d}", i));
  }
}

// 验证 L0→L1 自动压缩触发系列设置了4倍比例
// 当 L0 SST 达到 LSM_SST_LEVEL_RATIO（4个）时，触发压缩
TEST_F(LSMTest, L0L1Compaction_TriggeredAtLevelRatio) {
  // 每批 400KB 左右数据会填满一个 memtable（MAX_MEMTABLE_SIZE_PER_TABLE = 1MB）
  // 多写几个 flush 来累积 L0 SST，触发压缩
  const int records_per_flush = 10000;
  const int num_flushes = 5;  // 超过 LSM_SST_LEVEL_RATIO (4)

  for (int flush_idx = 0; flush_idx < num_flushes; ++flush_idx) {
    for (int i = 0; i < records_per_flush; ++i) {
      int global_idx = flush_idx * records_per_flush + i;
      lsm->put(std::format("l0_key_{:06d}", global_idx),
               std::format("l0_val_{:06d}", global_idx));
    }
    lsm->flush();
  }

  lsm->flush_all();

  // 验证所有写入的数据都还能读出
  for (int flush_idx = 0; flush_idx < num_flushes; ++flush_idx) {
    for (int i = 0; i < records_per_flush; i += 100) {  // 抽样验证
      int global_idx = flush_idx * records_per_flush + i;
      auto val = lsm->get(std::format("l0_key_{:06d}", global_idx));
      ASSERT_TRUE(val.has_value()) << "After L0→L1 compaction, l0_key_" << global_idx << " should
exist"; EXPECT_EQ(*val, std::format("l0_val_{:06d}", global_idx));
    }
  }
}

// 多层压缩：逐级写入数据触发 L1→L2→L3 压缩
TEST_F(LSMTest, MultiLevelCompaction_DataIntegrityAcrossLevels) {
  const int N = 100000;  // 大量数据

  for (int i = 0; i < N; ++i) {
    lsm->put(std::format("multi_key_{:06d}", i), std::format("multi_val_{:06d}", i));
  }

  // 强制全量 flush_all，触发多层压缩
  lsm->flush_all();

  // 随机验证数据完整性（每 1000 个取一个）
  for (int i = 0; i < N; i += 1000) {
    auto val = lsm->get(std::format("multi_key_{:06d}", i));
    ASSERT_TRUE(val.has_value()) << "multi_key_" << i << " lost after multi-level compaction";
    EXPECT_EQ(*val, std::format("multi_val_{:06d}", i));
  }
}
*/
// 覆盖写入大量 key 后，每个 key 仅保留最新值
TEST_F(LSMTest, OverwriteUnderLoad_OnlyLatestVisible) {
  const int N = 1000;
  // 第一轮写 v1
  for (int i = 0; i < N; ++i) {
    lsm->put(std::format("owkey_{:04d}", i), "v1");
  }
  lsm->flush();

  // 第二轮覆盖写 v2
  for (int i = 0; i < N; ++i) {
    lsm->put(std::format("owkey_{:04d}", i), "v2");
  }
  lsm->flush_all();

  for (int i = 0; i < N; ++i) {
    auto val = lsm->get(std::format("owkey_{:04d}", i));
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "v2") << "owkey_" << i << " should be overwritten to v2";
  }
}

// 压缩过程中删除数据，验证删除标记正确处理
TEST_F(LSMTest, CompactionWithDeletes_DeletesPreserved) {
  const int N = 3000;

  // 第一阶段：写入数据
  for (int i = 0; i < N; ++i) {
    lsm->put(std::format("del_key_{:05d}", i), std::format("value_{:05d}", i));
  }
  lsm->flush();

  // 第二阶段：删除一部分数据
  for (int i = 0; i < N; i += 2) {
    lsm->remove(std::format("del_key_{:05d}", i));
  }
  lsm->flush();

  // 第三阶段：再写入新数据，触发压缩
  for (int i = 0; i < N / 2; ++i) {
    lsm->put(std::format("new_key_{:05d}", i), "new_val");
  }
  lsm->flush_all();

  // 验证：
  // 1. 奇数索引的 key 应该存在
  for (int i = 1; i < N; i += 2) {
    auto val = lsm->get(std::format("del_key_{:05d}", i));
    ASSERT_TRUE(val.has_value()) << "del_key_" << i << " (odd) should exist after compaction";
  }

  // 2. 偶数索引的 key 应该不存在
  for (int i = 0; i < N; i += 2) {
    auto val = lsm->get(std::format("del_key_{:05d}", i));
    EXPECT_FALSE(val.has_value()) << "del_key_" << i << " (even) should be deleted";
  }

  // 3. 新插入的数据应该可见
  for (int i = 0; i < N / 2; i += 100) {
    auto val = lsm->get(std::format("new_key_{:05d}", i));
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "new_val");
  }
}

// ─────────────────────────────────────────────
//  5. 事务隔离（基础场景）
// ─────────────────────────────────────────────
/*
// 未提交事务的写入在事务外不可见（快照隔离）
TEST_F(LSMTest, Transaction_UncommittedWriteNotVisibleOutside) {
  lsm->put("shared_key", "initial");

  auto tran = lsm->begin_tran(IsolationLevel::SnapshotIsolation);
  // 事务内写入（不提交）
  tran->put("shared_key", "modified_in_tran");

  // 事务外查询应看到旧值
  auto outside_val = lsm->get("shared_key");
  ASSERT_TRUE(outside_val.has_value());
  EXPECT_EQ(*outside_val, "initial") << "Uncommitted write should not be visible outside
transaction";

  tran->rollback();
}

// 提交事务后，写入在事务外可见
TEST_F(LSMTest, Transaction_CommittedWriteBecomesVisible) {
  lsm->put("tx_key", "before");

  {
    auto tran = lsm->begin_tran(IsolationLevel::SnapshotIsolation);
    tran->put("tx_key", "after");
    tran->commit();
  }

  auto val = lsm->get("tx_key");
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, "after") << "Committed write should be visible";
}

// 事务回滚后数据应保持原样
TEST_F(LSMTest, Transaction_RollbackRestoresOriginalValue) {
  lsm->put("rb_key", "original");

  {
    auto tran = lsm->begin_tran(IsolationLevel::SnapshotIsolation);
    tran->put("rb_key", "should_not_persist");
    tran->rollback();
  }

  auto val = lsm->get("rb_key");
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, "original") << "Rollback should restore original value";
}

// 两个并发事务写不同 key，互不干扰
TEST_F(LSMTest, Transaction_ConcurrentWritesDifferentKeys_NoConflict) {
  auto tran_a = lsm->begin_tran(IsolationLevel::SnapshotIsolation);
  auto tran_b = lsm->begin_tran(IsolationLevel::SnapshotIsolation);

  tran_a->put("key_a", "value_a");
  tran_b->put("key_b", "value_b");

  EXPECT_NO_THROW(tran_a->commit());
  EXPECT_NO_THROW(tran_b->commit());

  EXPECT_EQ(*lsm->get("key_a"), "value_a");
  EXPECT_EQ(*lsm->get("key_b"), "value_b");
}
*/
// ─────────────────────────────────────────────
//  6. 迭代器基本遍历
// ─────────────────────────────────────────────

// 写入有序 key 后，迭代器应按字典序升序遍历且无遗漏
/*
TEST_F(LSMTest, Iterator_TraversesAllKeysInOrder) {
  const int N = 100;
  std::vector<std::string> expected_keys;
  for (int i = 0; i < N; ++i) {
    std::string k = std::format("iter_key_{:04d}", i);
    lsm->put(k, "v");
    expected_keys.push_back(k);
  }
  std::sort(expected_keys.begin(), expected_keys.end());
  lsm->flush();

  // 使用一个已提交事务 id 作为快照（用全量可见时间戳）
  constexpr uint64_t SNAPSHOT_TS = std::numeric_limits<uint64_t>::max();
  auto it  = lsm->begin(SNAPSHOT_TS);
  auto end = lsm->end();

  std::vector<std::string> actual_keys;
  for (; it != end; ++it) {
    auto [k, v] = it.getValue();
    if (k.starts_with("iter_key_")) {
      actual_keys.push_back(k);
    }
  }

  ASSERT_EQ(actual_keys.size(), expected_keys.size()) << "Iterator should visit all keys";
  EXPECT_EQ(actual_keys, expected_keys) << "Keys should be in sorted order";
}

// 压缩后迭代器仍能正常遍历所有数据
TEST_F(LSMTest, Iterator_AfterCompaction_TraversesAllData) {
  const int N = 2000;

  // 分多批写入并 flush，触发压缩
  const int batch_size = 500;
  for (int batch = 0; batch < 4; ++batch) {
    for (int i = 0; i < batch_size; ++i) {
      int idx = batch * batch_size + i;
      lsm->put(std::format("cmp_key_{:05d}", idx), std::format("val_{:05d}", idx));
    }
    lsm->flush();
  }
  lsm->flush_all();

  // 通过迭代器遍历所有数据
  constexpr uint64_t SNAPSHOT_TS = std::numeric_limits<uint64_t>::max();
  auto it = lsm->begin(SNAPSHOT_TS);
  auto end = lsm->end();

  std::vector<std::string> actual_keys;
  for (; it != end; ++it) {
    auto [k, v] = it.getValue();
    if (k.starts_with("cmp_key_")) {
      actual_keys.push_back(k);
    }
  }

  // 验证：数据完整且有序
  ASSERT_EQ(actual_keys.size(), N) << "Iterator should find all keys after compaction";

  // 检查是否有序
  for (size_t i = 1; i < actual_keys.size(); ++i) {
    EXPECT_LT(actual_keys[i - 1], actual_keys[i])
        << "Keys should be in sorted order, but " << actual_keys[i - 1]
        << " >= " << actual_keys[i];
  }
}
*/
// ─────────────────────────────────────────────
//  7. 批量操作与压缩交互
// ─────────────────────────────────────────────

// 大量 batch 操作触发多次压缩
TEST_F(LSMTest, BatchOperations_WithCompaction) {
  const int batch_size  = 500;
  const int num_batches = 10;

  std::vector<std::pair<std::string, std::string>> all_kvs;

  for (int batch = 0; batch < num_batches; ++batch) {
    std::vector<std::pair<std::string, std::string>> batch_kvs;
    for (int i = 0; i < batch_size; ++i) {
      int         idx = batch * batch_size + i;
      std::string k   = std::format("batch_cmp_key_{:05d}", idx);
      std::string v   = std::format("batch_cmp_val_{:05d}", idx);
      batch_kvs.push_back({k, v});
      all_kvs.push_back({k, v});
    }
    lsm->put_batch(batch_kvs);
    lsm->flush();
  }
  lsm->flush_all();

  // 验证所有数据
  for (const auto& [k, v] : all_kvs) {
    auto result = lsm->get(k);
    ASSERT_TRUE(result.has_value()) << "Key " << k << " lost after batch+compaction";
    EXPECT_EQ(*result, v);
  }
}

TEST_F(LSMTest, GetBatch_AfterCompaction_CorrectResults) {
  const int N = 1000;

  // 写入和压缩
  std::vector<std::string> all_keys;
  for (int i = 0; i < N; ++i) {
    std::string k = std::format("gbatch_key_{:04d}", i);
    lsm->put(k, std::format("val_{:04d}", i));
    all_keys.push_back(k);
  }
  lsm->flush_all();

  // 批量查询混合存在和不存在的 key
  std::vector<std::string> query_keys;
  for (int i = 0; i < N; i += 2) {
    query_keys.push_back(all_keys[i]);
  }
  // 添加一些不存在的 key
  query_keys.push_back("nonexist_1");
  query_keys.push_back("nonexist_2");

  auto results = lsm->get_batch(query_keys);

  size_t found_count = 0;
  size_t not_found_count = 0;

  for (const auto& [key, val] : results) {
    if (val.has_value()) {
      found_count++;
      EXPECT_TRUE(key.starts_with("gbatch_key_"));
    } else {
      not_found_count++;
      EXPECT_TRUE(key.starts_with("nonexist"));
    }
  }

  EXPECT_EQ(found_count, N / 2) << "Should find half the keys";
  EXPECT_EQ(not_found_count, 2) << "Should not find 2 nonexistent keys";
}

// ─────────────────────────────────────────────
//  9. clear() 语义
// ─────────────────────────────────────────────

// clear 后仍可写入新数据//
TEST_F(LSMTest, Clear_CanWriteAfterClear) {
  lsm->put("before_clear", "old");
  lsm->clear();
  lsm->put("after_clear", "new");

  EXPECT_FALSE(lsm->get("before_clear").has_value());
  auto val = lsm->get("after_clear");
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, "new");
}

// clear 整个压缩后的数据库
TEST_F(LSMTest, Clear_AfterCompaction_RemovesAllData) {
  const int N = 5000;

  // 写入并压缩
  for (int i = 0; i < N; ++i) {
    lsm->put(std::format("clr_cmp_key_{:05d}", i), "v");
    if ((i + 1) % 1000 == 0) {
      lsm->flush();
    }
  }
  lsm->flush_all();

  // 验证数据存在
  ASSERT_TRUE(lsm->get("clr_cmp_key_00000").has_value());

  // 清空
  lsm->clear();

  // 验证全部消失
  for (int i = 0; i < N; i += 100) {
    EXPECT_FALSE(lsm->get(std::format("clr_cmp_key_{:05d}", i)).has_value())
        << "After clear, all compacted data should be gone";
  }
}

// ─────────────────────────────────────────────
//  10. 边界与特殊操作测试
// ─────────────────────────────────────────────
/*
// 大值处理：验证较大的 value 能正确存贮和读取
TEST_F(LSMTest, LargeValues_CorrectlyHandled) {
  const int N = 100;
  std::string large_value(100000, 'x');  // 100KB value

  for (int i = 0; i < N; ++i) {
    lsm->put(std::format("large_key_{:03d}", i), large_value);
  }
  lsm->flush_all();

  for (int i = 0; i < N; ++i) {
    auto val = lsm->get(std::format("large_key_{:03d}", i));
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, large_value);
  }
}
*/
// 特殊字符 key 处理
TEST_F(LSMTest, SpecialCharacterKeys_Handled) {
  std::vector<std::string> special_keys = {
      "key\nwith\nnewlines", "key\twith\ttabs", "key with spaces", "key/with/slashes",
      "key:with:colons",     "日本語キー",      "emoji🎉key"};

  for (const auto& k : special_keys) {
    lsm->put(k, "special_val");
  }
  lsm->flush_all();

  for (const auto& k : special_keys) {
    auto val = lsm->get(k);
    ASSERT_TRUE(val.has_value()) << "Should handle key: " << k;
    EXPECT_EQ(*val, "special_val");
  }
}

// 反复覆盖同一个 key 很多次
TEST_F(LSMTest, RepeatedOverwrites_OnlyLatestVisible) {
  const int   N   = 1000;
  std::string key = "overwrite_test_key";

  for (int i = 0; i < N; ++i) {
    lsm->put(key, std::format("version_{}", i));
  }

  auto val = lsm->get(key);
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, std::format("version_{}", N - 1));
}

// 混合操作：写、删、写同一个 key
TEST_F(LSMTest, MixedOperations_WriteDeleteWrite) {
  const std::string key = "mixed_op_key";

  lsm->put(key, "v1");
  lsm->flush();

  ASSERT_EQ(*lsm->get(key), "v1");

  lsm->remove(key);
  lsm->flush();

  ASSERT_FALSE(lsm->get(key).has_value());

  lsm->put(key, "v2");
  lsm->flush();

  ASSERT_EQ(*lsm->get(key), "v2");
}

// 空 value 处理
TEST_F(LSMTest, EmptyValues_CorrectlyHandled) {
  const int N = 100;

  for (int i = 0; i < N; ++i) {
    lsm->put(std::format("empty_key_{:03d}", i), "");
  }
  lsm->flush_all();

  for (int i = 0; i < N; ++i) {
    auto val = lsm->get(std::format("empty_key_{:03d}", i));
    ASSERT_FALSE(val.has_value());
  }
}

// ─────────────────────────────────────────────
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}