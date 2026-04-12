#include "../../include/storage/file.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>
#include <iostream>
#include <cstdio>

bool StdFile::open(const std::string& filename, bool create) {
  filename_ = filename;

  int flags = create ? (O_RDWR | O_CREAT | O_TRUNC) : O_RDWR;
  fd_       = ::open(filename.c_str(), flags, 0644);

  if (fd_ < 0) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    perror("Error");
    return false;
  }
  return true;
}

bool StdFile::is_open() const {
  return fd_ >= 0;
}

bool StdFile::create(const std::string& filename, const std::vector<uint8_t>& data) {
  if (!open(filename, true)) {
    return false;
  }
  if (!data.empty()) {
    ssize_t n = ::pwrite(fd_, data.data(), data.size(), 0);
    if (n < 0 || static_cast<size_t>(n) != data.size()) {
      return false;
    }
    ::fsync(fd_);
  }
  return true;
}

// pread 不修改文件偏移，多线程并发读安全
std::vector<uint8_t> StdFile::read(size_t offset, size_t length) {
  size_t file_size = size();
  if (offset >= file_size)
    throw std::out_of_range("Offset beyond file size");

  size_t               read_size = std::min(length, file_size - offset);
  std::vector<uint8_t> buffer(read_size);

  ssize_t n = ::pread(fd_, buffer.data(), read_size, static_cast<off_t>(offset));
  if (n < 0)
    throw std::runtime_error("pread failed");

  buffer.resize(static_cast<size_t>(n));
  return buffer;
}

void StdFile::close() {
  if (fd_ >= 0) {
    ::fsync(fd_);
    ::close(fd_);
    fd_ = -1;
  }
}

size_t StdFile::size() const {
  struct stat st;
  if (::fstat(fd_, &st) < 0)
    throw std::runtime_error("fstat failed");
  return static_cast<size_t>(st.st_size);
}

bool StdFile::write(size_t offset, const void* data, size_t sz) {
  ssize_t n = ::pwrite(fd_, data, sz, static_cast<off_t>(offset));
  return n == static_cast<ssize_t>(sz);
}

bool StdFile::sync() {
  return ::fsync(fd_) == 0;
}

bool StdFile::remove() {
  close();
  return std::remove(filename_.c_str()) == 0;
}