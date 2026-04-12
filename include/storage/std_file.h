#pragma once
#include <cstddef>
#include <filesystem>
#include <string>
#include <vector>

class StdFile {
 public:
  StdFile() {}
  ~StdFile() {
    if (is_open()) {
      close();
    }
  }

  // 禁止拷贝，允许移动
  StdFile(const StdFile&)            = delete;
  StdFile& operator=(const StdFile&) = delete;
  StdFile(StdFile&& other) noexcept : fd_(other.fd_), filename_(std::move(other.filename_)) {
    other.fd_ = -1;
  }
  StdFile& operator=(StdFile&& other) noexcept {
    if (this != &other) {
      close();
      fd_       = other.fd_;
      filename_ = std::move(other.filename_);
      other.fd_ = -1;
    }
    return *this;
  }

  bool                 open(const std::string& filename, bool create);
  bool                 is_open() const;
  bool                 create(const std::string& filename, const std::vector<uint8_t>& buf);
  std::vector<uint8_t> read(size_t offset, size_t length);
  void                 close();
  size_t               size() const;
  bool                 write(size_t offset, const void* data, size_t size);
  bool                 sync();
  bool                 remove();

 private:
  int                   fd_ = -1;
  std::filesystem::path filename_;
};