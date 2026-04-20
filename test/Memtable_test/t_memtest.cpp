#include "../../include/core/memtable.h"
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <format>
#include <memory>
#include <print>
#include <string>
#include <thread>
#include <vector>

class MemtableTest : public ::testing::Test {
 protected:
  void SetUp() override { memtable = std::move(std::make_unique<MemTable>()); }

  std::unique_ptr<MemTable> memtable;

  // 预先生成测试键值对，避免测试中的构造开销
  std::vector<std::pair<std::string, std::string>> GenerateTestData(
      int count, std::string_view prefix = "key") {
    std::vector<std::pair<std::string, std::string>> data;
    data.reserve(count);

    for (int i = 0; i < count; ++i) {
      // 使用format一次性构造，避免多次分配
      data.emplace_back(std::format("{}{}", prefix, i), std::format("value{}", i));
    }
    return data;
  }

  // 生成带固定格式的测试数据（用于范围查询测试）
  std::vector<std::pair<std::string, std::string>> GenerateFormattedTestData(
      int count, std::string_view prefix = "key", std::string_view value_prefix = "value") {
    std::vector<std::pair<std::string, std::string>> data;
    data.reserve(count);

    for (int i = 0; i < count; ++i) {
      data.emplace_back(std::format("{}_{:03d}", prefix, i),
                        std::format("{}_{:03d}", value_prefix, i));
    }
    return data;
  }
};

// 基本的 put/get 操作测试
TEST_F(MemtableTest, BasicPutGet) {
  memtable->put_mutex("key1", "value1");
  auto result = memtable->get("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().first, "value1");
}

// 测试带锁的 put/get 操作
TEST_F(MemtableTest, MutexPutGet) {
  memtable->put_mutex("key1", "value1");
  std::vector<std::string> values;
  auto                     it = memtable->get("key1");
  EXPECT_TRUE(it.has_value());
  values.emplace_back(it.value().first);
  EXPECT_FALSE(values.empty());
  EXPECT_EQ(values[0], "value1");
}

TEST_F(MemtableTest, BatchOperations) {
  constexpr int batch_size = 100;
  auto          batch_data = GenerateTestData(batch_size);

  // 测试批量插入
  memtable->put_batch(batch_data);

  // 验证插入数量
  std::vector<std::string> keys;
  keys.reserve(batch_size);
  for (const auto& [key, _] : batch_data) {
    keys.push_back(key);
  }

  auto result = memtable->get_batch(keys);
  EXPECT_EQ(result.size(), batch_size);

  // 验证数据正确性 - 根据你的get_batch返回类型修正
  for (size_t i = 0; i < batch_size; ++i) {
    const auto&        tuple_result = result[i];
    const std::string& key          = std::get<0>(tuple_result);
    const auto&        opt_value    = std::get<1>(tuple_result);
    const auto&        opt_txid     = std::get<2>(tuple_result);

    EXPECT_EQ(key, std::format("key{}", i));
    EXPECT_TRUE(opt_value.has_value());
    if (opt_value.has_value()) {
      EXPECT_EQ(opt_value.value(), std::format("value{}", i));
    }
  }
}

// 删除操作测试
TEST_F(MemtableTest, RemoveOperations) {
  // 测试数据准备
  constexpr int test_count = 100;
  auto          test_data  = GenerateTestData(test_count);

  // 插入数据
  for (const auto& [key, value] : test_data) {
    memtable->put_mutex(key, value);
  }

  // 删除前50个键
  for (int i = 0; i < 50; ++i) {
    memtable->remove_mutex(std::format("key{}", i));
  }

  // 验证删除结果
  for (int i = 0; i < test_count; ++i) {
    auto key    = std::format("key{}", i);
    auto result = memtable->get(key);
    if (i < 50) {
      // 已删除的键应返回空值
      EXPECT_TRUE(result.has_value());
      EXPECT_TRUE(result.value().first.empty());
    } else {
      // 未删除的键应有值
      EXPECT_TRUE(result.has_value());
      EXPECT_EQ(result.value().first, std::format("value{}", i));
    }
  }
}

TEST_F(MemtableTest, TransactionIdTest) {
  // 测试不同事务ID的数据版本
  constexpr int txid1 = 100;
  constexpr int txid2 = 200;

  memtable->put("shared_key", "value1", txid1);
  memtable->put("shared_key", "value2", txid2);

  // 测试按事务ID获取
  std::vector<std::string> values;
  auto                     it1 = memtable->cur_get("shared_key", txid1);
  EXPECT_TRUE(it1.valid());
  values.push_back(it1.getValue().second);
  EXPECT_EQ(values.back(), "value1");

  auto it2 = memtable->cur_get("shared_key", txid2);
  EXPECT_TRUE(it2.valid());
  values.push_back(it2.getValue().second);
  EXPECT_EQ(values.back(), "value2");
}

// 表冻结和刷新测试
TEST_F(MemtableTest, FrozenAndFlush) {
  constexpr int test_count = 1000;
  auto          test_data  = GenerateTestData(test_count);

  // 插入数据
  size_t total_size = 0;
  for (const auto& [key, value] : test_data) {
    memtable->put(key, value);
    total_size += key.size() + value.size();
  }

  size_t initial_size = memtable->get_cur_size();
  EXPECT_GT(initial_size, 0);

  // 冻结表
  memtable->frozen_cur_table(true);

  // 验证冻结结果
  EXPECT_EQ(memtable->get_cur_size(), 0);
  EXPECT_EQ(memtable->get_fixed_size(), initial_size);
  EXPECT_GE(memtable->get_fixed_size(), total_size);
}

// 范围查询测试 - 使用精确的范围验证
TEST_F(MemtableTest, RangeSearchTest) {
  constexpr int              num_records = 100;
  constexpr std::string_view prefix      = "test_key";

  // 使用修正后的方法生成测试数据
  auto test_data = GenerateFormattedTestData(num_records, prefix, "value");

  // 插入测试数据
  for (const auto& [key, value] : test_data) {
    memtable->put(key, value);
  }

  // 执行范围查询
  auto range_iter = memtable->prefix_serach(std::string(prefix) + "_");
  std::vector<std::pair<std::string, std::string>> results;
  int                                              num = 100;
  while (range_iter.valid() && num) {
    results.push_back(range_iter.getValue());
    ++range_iter;
    --num;
  }

  // 验证结果数量
  EXPECT_EQ(results.size(), num_records);

  // 验证每个结果 - 使用test_data作为预期值，确保一一对应
  for (size_t i = 0; i < results.size(); ++i) {
    const auto& [expected_key, expected_value] = test_data[i];
    const auto& [actual_key, actual_value]     = results[i];

    EXPECT_EQ(actual_key, expected_key);
    EXPECT_EQ(actual_value, expected_value);
  }

  // 测试不存在的前缀
  auto empty_iter = memtable->prefix_serach("nonexistent_prefix");
  EXPECT_FALSE(empty_iter.valid());
}

// 性能测试
TEST_F(MemtableTest, PerformanceAndMemoryUsageTest) {
  constexpr int num_records    = 10000;
  constexpr int warmup_records = 1000;

  // 预热
  auto warmup_data = GenerateTestData(warmup_records, "warmup_key");
  for (const auto& [key, value] : warmup_data) {
    memtable->put(key, value);
  }

  // 生成测试数据
  auto test_data = GenerateTestData(num_records);

  // 插入性能测试
  auto insert_start = std::chrono::high_resolution_clock::now();
  for (const auto& [key, value] : test_data) {
    memtable->put_mutex(key, value);
  }
  auto insert_end = std::chrono::high_resolution_clock::now();
  auto insert_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(insert_end - insert_start);

  std::print("插入 {} 条记录耗时: {} ms\n", num_records, insert_duration.count());

  // 查询性能测试
  auto query_start = std::chrono::high_resolution_clock::now();
  for (const auto& [key, _] : test_data) {
    auto result = memtable->get(key);
    EXPECT_TRUE(result.has_value());
  }
  auto query_end = std::chrono::high_resolution_clock::now();
  auto query_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(query_end - query_start);

  std::print("查询 {} 条记录耗时: {} ms\n", num_records, query_duration.count());

  // 删除性能测试
  auto delete_start = std::chrono::high_resolution_clock::now();
  for (const auto& [key, _] : test_data) {
    memtable->remove(key);
  }
  auto delete_end = std::chrono::high_resolution_clock::now();
  auto delete_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(delete_end - delete_start);

  std::print("删除 {} 条记录耗时: {} ms\n", num_records, delete_duration.count());

  // 内存使用统计
  size_t cur_num    = memtable->get_node_num();
  size_t cur_size   = memtable->get_cur_size();
  size_t fixed_size = memtable->get_fixed_size();
  size_t total_size = memtable->get_total_size();

  std::print("\n内存使用统计:\n");
  std::print("当前表大小: {} bytes\n", cur_num);
  std::print("当前表内存大小: {} bytes\n", cur_size);
  std::print("固定表内存大小: {} bytes\n", fixed_size);
  std::print("总内存大小: {} bytes\n", total_size);
}

// Benchmark WITH sharding
TEST_F(MemtableTest, ConcurrentPutGet_Benchmark_Sharding) {
  auto memtable_shard = std::make_unique<MemTable>();

  const int NUM_THREADS     = 10;
  const int KEYS_PER_THREAD = 100000;
  const int TOTAL_OPS       = NUM_THREADS * KEYS_PER_THREAD;

  std::vector<std::pair<std::string, std::string>> test_data(TOTAL_OPS);
  for (int t = 0; t < NUM_THREADS; ++t) {
    for (int i = 0; i < KEYS_PER_THREAD; ++i) {
      int idx        = t * KEYS_PER_THREAD + i;
      test_data[idx] = {std::format("noshard_t{:02d}_k{:05d}", t, i),
                        std::format("val_t{:02d}_i{:05d}", t, i)};
    }
  }

  std::atomic<int>         write_success{0};
  std::vector<std::thread> writers;

  auto write_start = std::chrono::steady_clock::now();
  for (int t = 0; t < NUM_THREADS; ++t) {
    writers.emplace_back([&, t]() {
      for (int i = 0; i < KEYS_PER_THREAD; ++i) {
        int idx = t * KEYS_PER_THREAD + i;
        memtable_shard->put_mutex(test_data[idx].first, test_data[idx].second);
        write_success.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto& w : writers) {
    w.join();
  }
  writers.clear();

  auto   write_end = std::chrono::steady_clock::now();
  double write_sec = std::chrono::duration<double>(write_end - write_start).count();
  double write_qps = TOTAL_OPS / write_sec;

  // Print actual shard node counts
  std::print("\n=== ACTUAL SHARD NODE COUNTS AFTER WRITE ===\n");
  auto   shard_counts = memtable_shard->getShardNodeCounts();
  size_t total_nodes  = 0;
  

  std::vector<std::thread> readers;
  std::atomic<int>         found_count{0};
  auto                     read_start = std::chrono::steady_clock::now();

  for (int t = 0; t < NUM_THREADS; ++t) {
    readers.emplace_back([&, t]() {
      for (int i = 0; i < KEYS_PER_THREAD; ++i) {
        int idx               = t * KEYS_PER_THREAD + i;
        auto& [key, expected] = test_data[idx];
        auto val              = memtable_shard->get(key);
        if (val.has_value() && val.value().first == expected) {
          found_count.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& r : readers) {
    r.join();
  }
  readers.clear();

  auto   read_end = std::chrono::steady_clock::now();
  double read_sec = std::chrono::duration<double>(read_end - read_start).count();
  double read_qps = TOTAL_OPS / read_sec;

  std::print("\n===SHARDING (open_shard=true) ===\n");
  std::print("Write: {} ops in {:.3f}s => {:.0f} QPS\n", TOTAL_OPS, write_sec, write_qps);
  std::print("Read:  {} ops in {:.3f}s => {:.0f} QPS\n", TOTAL_OPS, read_sec, read_qps);
  for (size_t i = 0; i < shard_counts.size(); ++i) {
    std::print("Shard[{}]: {} nodes\n", i, shard_counts[i]);
    total_nodes += shard_counts[i];
  }
  EXPECT_EQ(write_success.load(), TOTAL_OPS);
  EXPECT_EQ(found_count.load(), TOTAL_OPS);
}
TEST_F(MemtableTest, ConcurrentReadWrite_Correct_Sharding) {
    auto memtable_shard = std::make_unique<MemTable>();

    const int BASE_COUNT = 500'000;   // 预装载数量
    const int OP_COUNT   = 500'000;   // 并发操作数量
    const int NUM_THREADS = 10;       // 总线程数（5读5写）

    // ---- 1. 准备数据 ----
    std::vector<std::pair<std::string, std::string>> base_data(BASE_COUNT);
    std::vector<std::pair<std::string, std::string>> new_data(OP_COUNT);
    for (int i = 0; i < BASE_COUNT; ++i) {
        base_data[i] = {std::format("base_{:07d}", i), std::format("val_{:07d}", i)};
    }
    for (int i = 0; i < OP_COUNT; ++i) {
        new_data[i] = {std::format("new_{:07d}", i), std::format("val_{:07d}", i)};
    }

    // ---- 2. 预装载：保证读线程一定能读到数据 ----
    for (const auto& kv : base_data) {
        memtable_shard->put_mutex(kv.first, kv.second);
    }

    std::atomic<bool> start_flag{false};
    std::atomic<int> read_hits{0};
    std::vector<std::thread> workers;

    // ---- 3. 启动并发写线程 (写入全新 Key) ----
    for (int t = 0; t < NUM_THREADS / 2; ++t) {
        workers.emplace_back([&, t]() {
            while (!start_flag.load(std::memory_order_acquire)); 
            int chunk = OP_COUNT / (NUM_THREADS / 2);
            for (int i = t * chunk; i < (t + 1) * chunk; ++i) {
                memtable_shard->put_mutex(new_data[i].first, new_data[i].second);
            }
        });
    }

    // ---- 4. 启动并发读线程 (随机读预装载的 Key) ----
    for (int t = 0; t < NUM_THREADS / 2; ++t) {
        workers.emplace_back([&]() {
            std::mt19937 rng(std::random_device{}());
            std::uniform_int_distribution<int> dist(0, BASE_COUNT - 1);
            
            while (!start_flag.load(std::memory_order_acquire));
            
            for (int i = 0; i < (OP_COUNT / (NUM_THREADS / 2)); ++i) {
                int idx = dist(rng);
                auto val = memtable_shard->get(base_data[idx].first);
                
                // 此时，val 必须 has_value，因为是预装载的
                if (val.has_value() && val.value().first == base_data[idx].second) {
                    read_hits.fetch_add(1, std::memory_order_relaxed);
                } else {
                    // 如果进到这里，说明并发下的索引逻辑出 Bug 了
                    ADD_FAILURE() << "Base key should always be found!";
                }
            }
        });
    }

    // ---- 5. 计时开始 ----
    auto start_time = std::chrono::steady_clock::now();
    start_flag.store(true);
    for (auto& w : workers) w.join();
    auto end_time = std::chrono::steady_clock::now();

    // ---- 6. 结果汇报 ----
    double sec = std::chrono::duration<double>(end_time - start_time).count();
    std::print("Concurrent Mixed QPS: {:.0f}\n", (BASE_COUNT + OP_COUNT) / sec);
    EXPECT_EQ(read_hits.load(), OP_COUNT); // 校验读命中的次数是否等于预期的操作数
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}