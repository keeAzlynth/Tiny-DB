#include "../../include/core/memtable.h"
#include "../../include/storage/Sstable.h"
#include <gtest/gtest.h>
#include <memory>
#include <string>

class BlockMetaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 创建测试用的 BlockMeta 对象
    meta        = BlockMeta("first", "last", 1024);
    block_cache = std::make_shared<BlockCache>(4096, 2);
    memtable    = std::make_shared<MemTable>();
  }
  void TearDown() override {
    try {
      if (!tmp_path1.empty() && std::filesystem::exists(tmp_path1)) {
        std::filesystem::remove(tmp_path1);
      }
    } catch (...) {
    }
  }
  BlockMeta                   meta;
  std::shared_ptr<BlockCache> block_cache;
  std::shared_ptr<MemTable>   memtable;
  std::string                 tmp_path1 = "/root/LSM/tmp/lsm_test_BlockMeta.dat";
  std::shared_ptr<Sstable>    sst;
};

// 测试基本构造函数
TEST_F(BlockMetaTest, BasicConstructors) {
  const std::string key_prefix  = "k";
  const size_t      num_records = 500;   // 较大数据量
  const size_t      block_size  = 4096;  // 较小 block 容量以产生多个 block

  if (std::filesystem::exists(tmp_path1))
    std::filesystem::remove(tmp_path1);

  Sstbuild builder(block_size);
  for (size_t i = 0; i < num_records; ++i) {
    memtable->put(key_prefix + std::to_string(i), "value" + std::to_string(i));
  }

  try {
    auto res=memtable->flushtodisk();
    for (auto it=res->begin(); it!=res->end(); ++it) {
      builder.add(it.getValue().first, it.getValue().second, it.get_tranc_id());
    }
    sst             = builder.build(block_cache, tmp_path1, 0);
    auto asser_size = Global_::generateRandom(0, 499);
    for (auto i = 0; i < asser_size; ++i) {
      auto res = sst->KeyExists(key_prefix + std::to_string(Global_::generateRandom(0, 499)),0);
    }
  } catch (const std::exception& e) {
    FAIL() << "Build failed: " << e.what();
    return;
  }
}

// 测试编码和解码功能
TEST_F(BlockMetaTest, EncodeDecode) {
  std::vector<BlockMeta> test_metas = {BlockMeta("key1", "key2", 100),
                                       BlockMeta("key3", "key4", 200),
                                       BlockMeta("key5", "key6", 300)};

  auto encoded = meta.encode_meta_to_slice(test_metas);

  // 验证编码后的数据不为空
  ASSERT_FALSE(encoded.empty());

  // 测试解码
  auto decoded = meta.decode_meta_from_slice(encoded);
  ASSERT_EQ(decoded.size(), test_metas.size());

  // 验证解码后的数据正确性
  for (size_t i = 0; i < test_metas.size(); i++) {
    EXPECT_EQ(decoded[i].first_key_, test_metas[i].first_key_);
    EXPECT_EQ(decoded[i].last_key_, test_metas[i].last_key_);
    EXPECT_EQ(decoded[i].offset_, test_metas[i].offset_);
  }
}

// 测试边界情况
TEST_F(BlockMetaTest, EdgeCases) {
  // 测试空列表编码解码
  std::vector<BlockMeta> empty_metas;
  auto                   encoded = meta.encode_meta_to_slice(empty_metas);
  auto                   decoded = meta.decode_meta_from_slice(encoded);
  EXPECT_TRUE(decoded.empty());

  // 测试包含空字符串的元数据
  std::vector<BlockMeta> test_metas = {BlockMeta("", "", 0), BlockMeta("key", "", 100),
                                       BlockMeta("", "last", 200)};

  encoded = meta.encode_meta_to_slice(test_metas);
  decoded = meta.decode_meta_from_slice(encoded);

  ASSERT_EQ(decoded.size(), test_metas.size());
  EXPECT_TRUE(decoded[0].first_key_.empty());
  EXPECT_TRUE(decoded[0].last_key_.empty());
  EXPECT_EQ(decoded[0].offset_, 0);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}