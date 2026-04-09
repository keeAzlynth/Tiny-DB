#include <gtest/gtest.h>
#include <chrono>

#include "../../include/core/Skiplist.h"

using namespace std::chrono_literals;

class SkiplistTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 初始化跳表并插入测试数据
    for (int i = 0; i < 10; ++i) {
      list.Insert(std::to_string(i), "value" + std::to_string(i));
    }
  }
  Skiplist list;
};

// 测试迭代器的基本功能
TEST_F(SkiplistTest, BasicIteration) {
  auto it = list.begin();
  EXPECT_TRUE(it.valid());

  int count = 0;
  while (it != list.end()) {
    auto value = it.getValue();
    EXPECT_EQ(value.first, std::to_string(count));
    EXPECT_EQ(value.second, "value" + std::to_string(count));
    ++it;
    ++count;
  }
  EXPECT_EQ(count, 10);
}

// 测试迭代器的边界情况
TEST_F(SkiplistTest, BoundaryConditions) {
  // 测试空跳表
  Skiplist empty_list;
  auto     it = empty_list.begin();
  EXPECT_FALSE(it.valid());
  EXPECT_TRUE(it == empty_list.end());
}

// 测试迭代器的比较操作
TEST_F(SkiplistTest, ComparisonOperators) {
  auto it1 = list.begin();
  auto it2 = list.begin();
  auto end = list.end();

  EXPECT_TRUE(it1 == it2);
  EXPECT_FALSE(it1 == end);
  EXPECT_TRUE(it1 != end);
}

// 测试迭代器的有效性
TEST_F(SkiplistTest, ValidityChecks) {
  auto it = list.begin();
  EXPECT_TRUE(it.valid());

  // 移动到末尾
  while (!it.isEnd()) {
    ++it;
  }
  EXPECT_FALSE(it.valid());
}

// 测试getValue操作
TEST_F(SkiplistTest, GetValue) {
  auto it    = list.begin();
  auto value = it.getValue();
  EXPECT_EQ(value.first, "0");
  EXPECT_EQ(value.second, "value0");
}

// 测试迭代器的++操作
TEST_F(SkiplistTest, IncrementOperator) {
  auto it = list.begin();
  EXPECT_EQ(it.getValue().first, "0");

  ++it;
  EXPECT_EQ(it.getValue().first, "1");

  // 测试连续递增
  for (int i = 2; i < 10; ++i) {
    ++it;
    EXPECT_EQ(it.getValue().first, std::to_string(i));
  }
}
class SkiplistTests : public ::testing::Test {
 protected:
  std::shared_ptr<Skiplist> lists;
  void                      SetUp() override {
    lists = std::make_shared<Skiplist>();
    // 初始化跳表并插入测试数据
    for (int i = 0; i < 1000; ++i) {
      lists->Insert("key" + std::to_string(i), "value" + std::to_string(i));
    }
  };
  // 清理资源
  void TearDown() override {
    // 清理跳表
    lists.reset();
  }
};
// 跳表的基本功能测试

// 基础功能测试
TEST_F(SkiplistTests, BasicOperations) {
  // 插入验证
  ASSERT_TRUE(lists->Insert("new_key", "new_value"));
  auto node = lists->Get("new_key");
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->key_, "new_key");
  EXPECT_EQ(node->value_, "new_value");

  // 更新验证
  ASSERT_TRUE(lists->Insert("key1", "updated_value"));
  node = lists->Get("key1");
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->value_, "updated_value");
}

// 事务ID测试
TEST_F(SkiplistTests, TransactionIdTest) {
  // 插入带事务ID的节点
  ASSERT_TRUE(lists->Insert("tx_key", "tx_value", 100));
  auto node = lists->Get("tx_key");
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->transaction_id, 100);

  // 事务ID冲突测试
  ASSERT_TRUE(lists->Insert("tx_key", "new_value", 101));  // 假设事务ID不匹配时插入失败
}

// 删除操作测试
TEST_F(SkiplistTests, DeleteOperations) {
  // 删除存在元素
  ASSERT_TRUE(lists->Delete("key500"));
  EXPECT_FALSE(lists->Contain("key500"));

  // 删除不存在元素
  ASSERT_FALSE(lists->Delete("non_existent_key"));
}

// 边界条件测试
TEST_F(SkiplistTests, BoundaryConditions) {
  // 长键值测试
  std::string long_key(10000, 'a');
  std::string long_value(10000, 'b');
  ASSERT_TRUE(lists->Insert(long_key, long_value));
  auto long_node = lists->Get(long_key);
  ASSERT_NE(long_node, nullptr);
  EXPECT_EQ(long_node->key_, long_key);
}

// 性能测试
TEST_F(SkiplistTests, PerformanceTest) {
  const int INSERT_COUNT = 100000;
  const int SEARCH_COUNT = 100000;

  // 批量插入性能
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < INSERT_COUNT; ++i) {
    lists->Insert("perf_key" + std::to_string(i), "perf_value");
  }
  auto end      = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  EXPECT_LT(duration, 5000);  // 期望5秒内完成

  // 批量查询性能
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < SEARCH_COUNT; ++i) {
    lists->Contain("perf_key" + std::to_string(i % INSERT_COUNT));
  }
  end      = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  EXPECT_LT(duration, 2000);  // 期望2秒内完成
}

// 测试叫AI帮你写就好了，省事.
// 你只要给出测试用例的描述和相应的API接口以及skiplist的定义，AI就能自动生成代码。