#include "../../include/storage/file.h"
#include <cstring>

FileObj::FileObj() : m_file(std::make_unique<StdFile>()), m_size(0) {}

FileObj::~FileObj() = default;

FileObj::FileObj(FileObj&& other) noexcept : m_file(std::move(other.m_file)), m_size(other.m_size) {
  other.m_size = 0;
}

FileObj& FileObj::operator=(FileObj&& other) noexcept {
  if (this != &other) {
    m_file       = std::move(other.m_file);
    m_size       = other.m_size;
    other.m_size = 0;
  }
  return *this;
}

size_t FileObj::size() const {
  return m_file ? m_file->size() : 0;
}

void FileObj::set_size(size_t new_size) {
  m_size = new_size;
}

void FileObj::del_file() {
  if (m_file) {
    m_file->remove();
  }
}

FileObj FileObj::create_and_write(const std::string& file_path, std::vector<uint8_t> buffer) {
  FileObj file_object;

  const bool create_success = file_object.m_file->create(file_path, buffer);
  if (!create_success) {
    throw std::runtime_error("Failed to create or write file: " + file_path);
  }

  file_object.m_file->sync();
  return file_object;
}

FileObj FileObj::open(const std::string& file_path, bool should_create) {
  FileObj file_object;

  const bool open_success = file_object.m_file->open(file_path, should_create);
  if (!open_success) {
    throw std::runtime_error("Failed to open file: " + file_path);
  }

  return file_object;
}

std::vector<uint8_t> FileObj::read_to_slice(size_t offset, size_t length) {
  const size_t file_size = m_file->size();

  // Check bounds to prevent reading beyond file size
  if (offset > file_size) {
    throw std::out_of_range("Read offset beyond file size");
  }

  if (offset + length > file_size) {
    throw std::out_of_range("Read range extends beyond file size");
  }

  return m_file->read(offset, length);
}

uint8_t FileObj::read_uint8(size_t offset) {
  validate_read_bounds(offset, sizeof(uint8_t));

  const auto data = m_file->read(offset, sizeof(uint8_t));
  return data[0];
}

uint16_t FileObj::read_uint16(size_t offset) {
  validate_read_bounds(offset, sizeof(uint16_t));

  const auto data = m_file->read(offset, sizeof(uint16_t));
  uint16_t   value;
  std::memcpy(&value, data.data(), sizeof(uint16_t));
  return value;
}

uint32_t FileObj::read_uint32(size_t offset) {
  validate_read_bounds(offset, sizeof(uint32_t));

  const auto data = m_file->read(offset, sizeof(uint32_t));
  uint32_t   value;
  std::memcpy(&value, data.data(), sizeof(uint32_t));
  return value;
}

uint64_t FileObj::read_uint64(size_t offset) {
  validate_read_bounds(offset, sizeof(uint64_t));

  const auto data = m_file->read(offset, sizeof(uint64_t));
  uint64_t   value;
  std::memcpy(&value, data.data(), sizeof(uint64_t));
  return value;
}

bool FileObj::write(size_t offset, std::vector<uint8_t>& buffer) {
  if (!m_file) {
    return false;
  }

  return m_file->write(offset, buffer.data(), buffer.size());
}

bool FileObj::append(std::vector<uint8_t>& buffer) {
  if (!m_file) {
    return false;
  }

  const size_t current_file_size = m_file->size();
  const bool   write_success     = m_file->write(current_file_size, buffer.data(), buffer.size());

  if (!write_success) {
    return false;
  }

  m_size += buffer.size();
  return true;
}

bool FileObj::is_open() {
  if (!m_file) {
    return false;
  }
  return m_file->is_open();
}
bool FileObj::sync() {
  if (!m_file) {
    return false;
  }

  return m_file->sync();
}
void FileObj::close() {
  if (m_file) {
    m_file->close();
  }
}

void FileObj::validate_read_bounds(size_t offset, size_t data_size) const {
  const size_t file_size = m_file->size();

  if (offset + data_size > file_size) {
    throw std::out_of_range("Read operation would exceed file boundaries");
  }
}
