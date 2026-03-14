#include "../../include/Block.h"
#include "../../include/BlockIterator.h"
#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <string>
#include <iostream>
#include <tuple>

class BlockTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 在 SetUp 中初始化，确保 Block 对象完全构造
    block = std::make_shared<Block>(4096);
  }
  void                   TearDown() override { block.reset(); }
  std::shared_ptr<Block> block;
};

// 测试基本操作
TEST_F(BlockTest, BasicOperations) {
  // 测试添加条目
  EXPECT_TRUE(block->add_entry("key1", "value1", 0));
  EXPECT_TRUE(block->add_entry("key2", "value2", 0));

  // 测试获取值
  auto value1 = block->get_value_binary("key1");
  EXPECT_TRUE(value1.has_value());
  EXPECT_EQ(value1.value(), "value1");
}

// 测试二分查找
TEST_F(BlockTest, BinarySearch) {
  block->add_entry("key1", "value1", 0);
  block->add_entry("key2", "value2", 0);
  block->add_entry("key3", "value3", 0);

  auto idx = block->get_offset_binary("key2");
  EXPECT_TRUE(idx.has_value());
}

// 测试编码解码
TEST_F(BlockTest, EncodeAndDecode) {
  block->add_entry("key1", "value1", 0);
  block->add_entry("key2", "value2", 0);

  auto encoded = block->encode();
  auto decoded = block->decode(encoded);

  EXPECT_EQ(decoded->get_value_binary("key1").value(), "value1");
  EXPECT_EQ(decoded->get_value_binary("key2").value(), "value2");
}

// 测试获取首尾键
TEST_F(BlockTest, FirstAndLastKey) {
  block->add_entry("key1", "value1", 0);
  block->add_entry("key2", "value2", 0);

  auto keys = block->get_first_and_last_key();
  EXPECT_EQ(keys.first, "key1");
  EXPECT_EQ(keys.second, "key2");
}

// 测试迭代器
TEST_F(BlockTest, Iterator) {
  // 添加调试信息，确认block正确创建
  ASSERT_NE(block, nullptr) << "Block should not be null";

  // 生成120个key的测试数据，按字典序排列（约能放入4096字节Block）
  std::vector<std::pair<std::string, std::string>> test_data;
  for (int i = 1; i <= 120; i++) {
    test_data.push_back({"key" + std::to_string(i), "value" + std::to_string(i)});
  }
  // 按字典序排序（字符串比较）
  std::sort(test_data.begin(), test_data.end());

  // 插入测试数据
  for (const auto& [key, value] : test_data) {
    ASSERT_TRUE(block->add_entry(key, value, 0)) << "Failed to add entry: " << key;
  }

  try {
    auto it  = block->begin();
    auto end = block->end();
    std::cout << it.getIndex() << std::endl;
    std::cout << end.getIndex() << std::endl;
    // 检查迭代器是否有效
    int                                              count = 0;
    std::vector<std::pair<std::string, std::string>> retrieved_data;

    // 遍历并收集数据
    while (it != end) {
      auto entry = it.getValue();  // 假设getValue返回pair<string,string>
      retrieved_data.push_back(entry);
      count++;
      ++it;
    }

    // 验证数量
    EXPECT_EQ(count, test_data.size()) << "Iterator count mismatch";

    // 验证顺序和内容
    ASSERT_EQ(retrieved_data.size(), test_data.size())
        << "Retrieved data size (" << retrieved_data.size() << ") doesn't match test data size ("
        << test_data.size() << ")";

    // 只有当数量匹配时才验证内容
    if (retrieved_data.size() == test_data.size()) {
      for (size_t i = 0; i < retrieved_data.size(); ++i) {
        EXPECT_EQ(retrieved_data[i].first, test_data[i].first)
            << "Key mismatch at position " << i << "\nExpected: " << test_data[i].first
            << "\nActual: " << retrieved_data[i].first;
        EXPECT_EQ(retrieved_data[i].second, test_data[i].second)
            << "Value mismatch at position " << i << "\nExpected: " << test_data[i].second
            << "\nActual: " << retrieved_data[i].second;
      }
    } else {
      // 如果数量不匹配，打印详细的调试信息
      std::cout << "\nRetrieved data contents:" << std::endl;
      for (const auto& [key, value] : retrieved_data) {
        std::cout << "Key: " << key << ", Value: " << value << std::endl;
      }
    }

  } catch (const std::bad_weak_ptr& e) {
    FAIL() << "Bad weak ptr exception: " << e.what() << "\nBlock address: " << block.get()
           << "\nUse count: " << block.use_count();
  } catch (const std::exception& e) {
    FAIL() << "Unexpected exception: " << e.what();
  }

  // 测试迭代器的边界情况
  try {
    // 测试空迭代器
    auto empty_it = block->end();
    EXPECT_EQ(empty_it, block->end());

    // 测试++操作是否正确到达end
    auto it = block->begin();
    for (size_t i = 0; i < test_data.size(); ++i) {
      EXPECT_NE(it, block->end());
      ++it;
    }
    EXPECT_EQ(it, block->end());

  } catch (const std::exception& e) {
    FAIL() << "Exception in boundary test: " << e.what();
  }
}

// 测试大小限制
TEST_F(BlockTest, SizeLimit) {
  std::string large_value(1024, 'x');  // 1KB大小的值
  EXPECT_TRUE(block->add_entry("key1", large_value, 0));
  EXPECT_GT(block->get_cur_size(), 1024);
}

// 测试空块
TEST_F(BlockTest, EmptyBlock) {
  EXPECT_TRUE(block->is_empty());
  EXPECT_EQ(block->get_cur_size(), 2);
}

TEST_F(BlockTest, RangeSearch) {
  // 生成120个key的测试数据，按字典序排列
  std::vector<std::pair<std::string, std::string>> test_data;
  for (int i = 1; i <= 120; i++) {
    test_data.push_back({"key" + std::to_string(i), "value" + std::to_string(i)});
  }
  // 按字典序排序（字符串比较）
  std::sort(test_data.begin(), test_data.end());

  // 插入测试数据
  for (const auto& [key, value] : test_data) {
    ASSERT_TRUE(block->add_entry(key, value, 0)) << "Failed to add entry: " << key;
  }

  try {
    // 测试前缀"key1"的范围查询 (会匹配: key1, key10, key100-key109, key11-key19)
    auto range_iterators = block->get_prefix_iterator("key1");
    ASSERT_TRUE(range_iterators.has_value()) << "Range iterators for 'key1' should not be null";

    auto begin = range_iterators->first;
    auto end   = range_iterators->second;

    ASSERT_NE(begin, nullptr) << "Begin iterator should not be null";
    ASSERT_NE(end, nullptr) << "End iterator should not be null";

    std::vector<std::pair<std::string, std::string>> retrieved_data;

    while (*begin != *end) {
      auto entry = begin->getValue();
      retrieved_data.push_back(entry);
      ++(*begin);
    }

    // 验证所有返回的key都以"key1"开头
    for (const auto& [key, value] : retrieved_data) {
      EXPECT_TRUE(key.starts_with("key1")) << "Key " << key << " does not start with 'key1'";
    }

    // 应该包含: key1, key10-key19, key100-key109, key110-key119
    EXPECT_GE(retrieved_data.size(), 20u) << "Expected at least 20 keys starting with 'key1'";

    // 测试前缀"key9"的范围查询
    auto range_iterators2 = block->get_prefix_iterator("key9");
    ASSERT_TRUE(range_iterators2.has_value()) << "Range iterators for 'key9' should not be null";

    auto begin2 = range_iterators2->first;
    auto end2   = range_iterators2->second;

    std::vector<std::pair<std::string, std::string>> retrieved_data2;
    while (*begin2 != *end2) {
      auto entry = begin2->getValue();
      retrieved_data2.push_back(entry);
      ++(*begin2);
    }

    // 验证所有返回的key都以"key9"开头
    for (const auto& [key, value] : retrieved_data2) {
      EXPECT_TRUE(key.starts_with("key9")) << "Key " << key << " does not start with 'key9'";
    }

  } catch (const std::exception& e) {
    FAIL() << "Unexpected exception: " << e.what();
  }
}

TEST_F(BlockTest, RangeSearchandMVCC) {
  // 生成50个key，每个key有1-2个版本，模拟MVCC场景（总数据量约75条，能放入Block）
  std::vector<std::tuple<std::string, std::string, uint64_t>> test_data;
  for (int i = 1; i <= 50; i++) {
    std::string key = "key" + std::to_string(i);
    // 第一个版本
    test_data.emplace_back(key, "value_v1_" + std::to_string(i), 1000 + i);
    // 第二个版本（针对部分key）
    if (i % 2 == 0) {
      test_data.emplace_back(key, "value_v2_" + std::to_string(i), 2000 + i);
    }
  }
  // 按字典序排序
  std::sort(test_data.begin(), test_data.end());

  // 插入测试数据
  for (const auto& [key, value, tranc_id] : test_data) {
    ASSERT_TRUE(block->add_entry(key, value, tranc_id)) << "Failed to add entry: " << key;
  }

  try {
    // 测试MVCC查询：查询以"key1"为前缀的所有key（包括key1, key10-key19, key100-key109）
    auto result1 = block->get_prefix_tran_id("key1", 1050);

    // 验证查询结果格式
    for (const auto& [key, value, ts] : result1) {
      EXPECT_TRUE(key.starts_with("key1")) << "Key " << key << " should start with 'key1'";
      EXPECT_LE(ts, 1050u) << "Timestamp should be <= query timestamp";
    }

    // 测试MVCC查询：在时间戳2100时，应该看到key50的两个版本（1050和2050）
    auto result2 = block->get_prefix_tran_id("key50", 2100);

    // 验证查询结果都是key开头的键
    for (const auto& [key, value, ts] : result2) {
      EXPECT_TRUE(key.starts_with("key5")) << "Key should start with 'key5'";
      EXPECT_LE(ts, 2100u) << "Timestamp should be <= query timestamp";
    }

    // 测试以key2开头的所有key
    auto result3 = block->get_prefix_tran_id("key2", 1500);

    // 验证结果都以key2开头
    for (const auto& [key, value, ts] : result3) {
      EXPECT_TRUE(key.starts_with("key2")) << "Key " << key << " should start with 'key2'";
      EXPECT_LE(ts, 1500u) << "Timestamp should be <= query timestamp";
    }

    // 验证至少找到一些结果
    EXPECT_GE(result1.size(), 1u) << "Should find results for prefix 'key1'";
    EXPECT_GE(result3.size(), 1u) << "Should find results for prefix 'key2'";

  } catch (const std::exception& e) {
    FAIL() << "Unexpected exception: " << e.what();
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
