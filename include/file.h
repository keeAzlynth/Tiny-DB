#pragma once

#include "mmap.h"
#include "std_file.h"
#include <memory>

class FileObj {
 private:
  std::unique_ptr<StdFile> m_file;
  size_t                   m_size;

 public:
  FileObj();
  ~FileObj();

  // 禁用拷贝
  FileObj(const FileObj&)            = delete;
  FileObj& operator=(const FileObj&) = delete;

  // 实现移动语义
  FileObj(FileObj&& other) noexcept;

  FileObj& operator=(FileObj&& other) noexcept;

  // 文件大小
  size_t size() const;

  // 设置文件大小
  void set_size(size_t size);

  // 删除文件
  void del_file();

  // 创建文件对象, 并写入到磁盘
  static FileObj create_and_write(const std::string& path, std::vector<uint8_t> buf);

  // 打开文件对象
  static FileObj open(const std::string& path, bool create);

  // 读取并返回切片
  std::vector<uint8_t> read_to_slice(size_t offset, size_t length);

  // 读取 uint8_t
  uint8_t read_uint8(size_t offset);

  // 读取 uint16_t
  uint16_t read_uint16(size_t offset);

  // 读取 uint32_t
  uint32_t read_uint32(size_t offset);

  // 读取 uint64_t
  uint64_t read_uint64(size_t offset);

  // 写入到文件
  bool write(size_t offset, std::vector<uint8_t>& buf);

  // 追加写入到文件
  bool append(std::vector<uint8_t>& buf);

  bool sync();
  void close();

  void validate_read_bounds(size_t offset, size_t data_size) const;
};
