#pragma once
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

class StdFile {
 public:
  StdFile() {}
  ~StdFile() {
    if (file_.is_open()) {
      file_.flush();
      close();
    }
  }

  // 打开文件并映射到内存
  bool open(const std::string& filename, bool create);

  // 创建文件
  bool create(const std::string& filename, const std::vector<uint8_t>& buf);
  // 读取数据
  std::vector<uint8_t> read(size_t offset, size_t length);
  // 关闭文件
  void close();

  // 获取文件大小
  size_t size();

  // 写入数据
  bool write(size_t offset, const void* data, size_t size);

  // 同步到磁盘
  bool sync();

  // 删除文件
  bool remove();

 private:
  std::fstream          file_;
  std::filesystem::path filename_;
};
