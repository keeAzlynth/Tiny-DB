#include "../../include/LSM.h"
#include "../../include/iterator/LeveIterator.h"
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
    std::print("{}", val.value());
    EXPECT_EQ(val->starts_with("new_val"), true);
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
/*
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

  size_t found_count     = 0;
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
*/

TEST_F(LSMTest, ConcurrentInsert_WithCompaction_ReadCorrect) {
  const int NUM_THREADS     = 10;
  const int KEYS_PER_THREAD = 50000;
  const int TOTAL_OPS       = NUM_THREADS * KEYS_PER_THREAD;

  // ---- 准备阶段：预生成数据（避免 format 占用测试时间） ----
  std::vector<std::pair<std::string, std::string>> test_data(TOTAL_OPS);
  for (int t = 0; t < NUM_THREADS; ++t) {
    for (int i = 0; i < KEYS_PER_THREAD; ++i) {
      int idx        = t * KEYS_PER_THREAD + i;
      test_data[idx] = {std::format("concurrent_t{:02d}_k{:04d}", t, i),
                        std::format("val_t{:02d}_i{:04d}", t, i)};
    }
  }

  std::atomic<int>         write_success{0};
  std::vector<std::thread> writers;

  // ---- 1. 并发写入计时 ----
  auto write_start = std::chrono::steady_clock::now();

  for (int t = 0; t < NUM_THREADS; ++t) {
    writers.emplace_back([&, t]() {
      for (int i = 0; i < KEYS_PER_THREAD; ++i) {
        int idx = t * KEYS_PER_THREAD + i;
        lsm->put(test_data[idx].first, test_data[idx].second);
        write_success.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto& w : writers) w.join();

  auto   write_end = std::chrono::steady_clock::now();
  double write_sec = std::chrono::duration<double>(write_end - write_start).count();
  double write_qps = TOTAL_OPS / write_sec;

  std::cout << std::format("[PERF] Concurrent Write: {} ops in {:.3f}s => {:.0f} QPS\n", TOTAL_OPS,
                           write_sec, write_qps);

  // ---- 2. 并发读取计时 ----
  std::vector<std::thread> readers;
  std::atomic<int>         found_count{0};
  auto                     read_start = std::chrono::steady_clock::now();

  for (int t = 0; t < NUM_THREADS; ++t) {
    readers.emplace_back([&, t]() {
      for (int i = 0; i < KEYS_PER_THREAD; ++i) {
        int idx               = t * KEYS_PER_THREAD + i;
        auto& [key, expected] = test_data[idx];

        auto val = lsm->get(key);
        if (val.has_value()) {
          if (val.value() == expected) {
            found_count.fetch_add(1, std::memory_order_relaxed);
          } else {
            // 只有失败时才打印，避免干扰性能测试
            ADD_FAILURE() << "Value mismatch for key: " << key;
          }
        } else {
          ADD_FAILURE() << "Key not found: " << key;
        }
      }
    });
  }

  for (auto& r : readers) r.join();

  auto   read_end = std::chrono::steady_clock::now();
  double read_sec = std::chrono::duration<double>(read_end - read_start).count();
  double read_qps = TOTAL_OPS / read_sec;

  std::cout << std::format("[PERF] Concurrent Read:  {} ops in {:.3f}s => {:.0f} QPS\n", TOTAL_OPS,
                           read_sec, read_qps);

  // ---- 3. 最终校验 ----
  lsm->flush_all();  // 确保所有数据下刷

  EXPECT_EQ(write_success.load(), TOTAL_OPS);
  EXPECT_EQ(found_count.load(), TOTAL_OPS);
}
/*
TEST_F(LSMTest, PrefixScan_Mixed_WithCompaction) {
  const int    N = 200;
  std::mt19937 rng(42);

  // === 第一批写入，制造第一个版本 ===
  for (int i = 0; i < N; ++i) {
    lsm->put(std::format("alpha_{:04d}", i), std::format("val_alpha_v1_{:04d}", i));
    lsm->put(std::format("beta_{:04d}", i), std::format("val_beta_v1_{:04d}", i));
    lsm->put(std::format("gamma_{:04d}", i), std::format("val_gamma_v1_{:04d}", i));
  }
  // 混入随机噪声数据
  for (int i = 0; i < 300; ++i) {
    lsm->put(std::format("noise_{:06d}", rng() % 100000), std::format("noise_val_{}", i));
  }
  lsm->flush();  //

  // === 第二批：部分 key 覆盖写入，制造第二个版本 ===
  for (int i = 0; i < N; i += 2) {  // 偶数 key 覆盖
    lsm->put(std::format("alpha_{:04d}", i), std::format("val_alpha_v2_{:04d}", i));
    lsm->put(std::format("beta_{:04d}", i), std::format("val_beta_v2_{:04d}", i));
  }
  // 再混入随机噪声
  for (int i = 0; i < 300; ++i) {
    lsm->put(std::format("noise_{:06d}", rng() % 100000), std::format("noise_val_{}", i));
  }
  lsm->flush();  // →

  // === 第三批：删除部分 gamma，再写入新 gamma ===
  for (int i = 0; i < 50; ++i) {
    lsm->remove(std::format("gamma_{:04d}", i));  // 删除 gamma 0~49
  }
  for (int i = N; i < N + 50; ++i) {  // 新增 gamma 200~249
    lsm->put(std::format("gamma_{:04d}", i), std::format("val_gamma_v1_{:04d}", i));
  }
  // 再混入随机噪声
  for (int i = 0; i < 300; ++i) {
    lsm->put(std::format("noise_{:06d}", rng() % 100000), std::format("noise_val_{}", i));
  }
  lsm->flush();  // →

  // ===================== 验证 alpha_ =====================
  {
    auto results = lsm->get_prefix_range("alpha_");
    ASSERT_EQ(results.size(), N) << "alpha_ should have exactly N keys";

    std::string prev = "";
    for (auto& [k, v, tranc_id] : results) {
      EXPECT_TRUE(k.starts_with("alpha_")) << "Unexpected key in alpha scan: " << k;
      EXPECT_GT(k, prev) << "alpha_ keys not in order at: " << k;
      prev = k;

      int idx = std::stoi(k.substr(k.size() - 4));
      // 偶数 key 应该是 v2，奇数 key 应该是 v1
      if (idx % 2 == 0) {
        EXPECT_EQ(v, std::format("val_alpha_v2_{:04d}", idx)) << "Wrong version for: " << k;
      } else {
        EXPECT_EQ(v, std::format("val_alpha_v1_{:04d}", idx)) << "Wrong version for: " << k;
      }
    }
  }

  // ===================== 验证 beta_ =====================
  {
    auto results = lsm->get_prefix_range("beta_");
    lsm->print_level_range(0);
    std::print("first beta_ key: {}, last beta_ key: {}\n", std::get<0>(results.front()),
std::get<0>(results.back())); ASSERT_EQ(results.size(), N) << "beta_ should have exactly N keys";

    std::string prev = "";
    for (auto& [k, v, tranc_id] : results) {
      EXPECT_TRUE(k.starts_with("beta_")) << "Unexpected key in beta scan: " << k;
      EXPECT_GT(k, prev) << "beta_ keys not in order at: " << k;
      prev = k;

      int idx = std::stoi(k.substr(k.size() - 4));
      if (idx % 2 == 0) {
        EXPECT_EQ(v, std::format("val_beta_v2_{:04d}", idx)) << "Wrong version for: " << k;
      } else {
        EXPECT_EQ(v, std::format("val_beta_v1_{:04d}", idx)) << "Wrong version for: " << k;
      }
    }
  }

  // ===================== 验证 gamma_ =====================
  {
    auto results = lsm->get_prefix_range("gamma_");
    // gamma 0~49 被删除，50~199 保留，200~249 新增，共 200 条
    ASSERT_EQ(results.size(), 200) << "gamma_ should have 200 keys (50 deleted, 50 added)";

    std::string   prev = "";
    std::set<int> seen_idx;
    for (auto& [k, v, tranc_id] : results) {
      EXPECT_TRUE(k.starts_with("gamma_")) << "Unexpected key in gamma scan: " << k;
      EXPECT_GT(k, prev) << "gamma_ keys not in order at: " << k;
      prev = k;

      int idx = std::stoi(k.substr(k.size() - 4));
      EXPECT_GE(idx, 50) << "Deleted gamma key appeared: " << k;
      EXPECT_LT(idx, 250) << "Out of range gamma key: " << k;
      EXPECT_EQ(v, std::format("val_gamma_v1_{:04d}", idx)) << "Wrong value for: " << k;
      seen_idx.insert(idx);
    }

    // 确认 0~49 不存在，50~249 全部存在
    for (int i = 0; i < 50; ++i) {
      EXPECT_EQ(seen_idx.count(i), 0) << "Deleted gamma_" << i << " should not appear";
    }
    for (int i = 50; i < 250; ++i) {
      EXPECT_EQ(seen_idx.count(i), 1) << "gamma_" << i << " should be present";
    }
  }

  // ===================== 验证噪声不污染前缀 =====================
  {
    auto alpha_results = lsm->get_prefix_range("alpha_");
    auto beta_results  = lsm->get_prefix_range("beta_");
    auto gamma_results = lsm->get_prefix_range("gamma_");

    for (auto& [k, v, tranc_id] : alpha_results) {
      EXPECT_FALSE(k.starts_with("noise_")) << "Noise key leaked into alpha scan";
    }
    for (auto& [k, v, tranc_id] : beta_results) {
      EXPECT_FALSE(k.starts_with("noise_")) << "Noise key leaked into beta scan";
    }
    for (auto& [k, v, tranc_id] : gamma_results) {
      EXPECT_FALSE(k.starts_with("noise_")) << "Noise key leaked into gamma scan";
    }
  }

  // ===================== 验证不存在的前缀 =====================
  {
    auto results = lsm->get_prefix_range("nonexist_");
    EXPECT_TRUE(results.empty()) << "Nonexistent prefix should return empty";
  }
}
*/
// ─────────────────────────────────────────────
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}