#pragma once
#include "file.h"
#include "../core/Global.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <expected>
#include <map>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

// ─── Error type ───────────────────────────────────────────────────────────────
enum class WalError : uint8_t {
  kSyncFailed,
  kFileCreateFailed,
  kCorrupted,
  kIOError,
  kChecksumMismatch,
};

// ─── WAL entry ────────────────────────────────────────────────────────────────
// Payload on disk (all little-endian):
//   key_len  : uint32
//   key      : key_len bytes
//   value_len: uint32   (empty string = tombstone, caller's convention)
//   value    : value_len bytes
//   tranc_id : uint64
struct WalEntry {
  std::string key;
  std::string value;
  uint64_t    tranc_id{0};
};

// ─── WAL ──────────────────────────────────────────────────────────────────────
//
//  Physical format: LevelDB-style 32 KB blocks.
//
//    Block layout:
//      [record]* [padding to block boundary]
//
//    Record header (7 bytes):
//      masked_crc32c : uint32  (covers type + data)
//      length        : uint16
//      type          : uint8   (kFull | kFirst | kMiddle | kLast)
//
//    Large entries are split across blocks.  The type field lets recovery
//    detect "clean truncation" per RocksDB issue #12488 – an incomplete
//    kFirst/kMiddle sequence has no matching kLast and is discarded on
//    recovery rather than silently accepted.
//
//  Concurrency:
//    kPipelined  – writers form a group; the leader writes WAL for the whole
//                  batch, fsyncs, then releases write_mutex_.  The next group
//                  can start WAL immediately while the previous group writes
//                  MemTable in parallel.
//    kUnordered  – each writer independently holds write_mutex_ for its own
//                  entry only; no grouping overhead.
// ─────────────────────────────────────────────────────────────────────────────
class WAL {
 public:
  explicit WAL(std::string_view log_dir, uint64_t checkpoint_tranc_id,
               uint64_t clean_interval_s = Global_::WAL_CLEAN_INTERVAL_S,
               uint64_t file_size_limit  = Global_::WAL_FILE_LIMIT);
  ~WAL();

  WAL(const WAL&)            = delete;
  WAL& operator=(const WAL&) = delete;
  WAL(WAL&&)                 = delete;
  WAL& operator=(WAL&&)      = delete;

  // Scan log_dir and return entries with tranc_id > checkpoint_tranc_id.
  // Returns kChecksumMismatch on detected corruption.
  [[nodiscard]] static std::expected<std::map<uint64_t, std::vector<WalEntry>>, WalError> recover(
      std::string_view log_dir, uint64_t checkpoint_tranc_id);

  // Append one entry and block until it is fsync'd to disk.
  // Dispatches to pipelined or unordered path based on WAL_WRITE_POLICY.
  [[nodiscard]] std::expected<void, WalError> log(const WalEntry& entry);

  // Force fsync without appending (used on commit / destructor).
  [[nodiscard]] std::expected<void, WalError> flush();

  void set_checkpoint_tranc_id(uint64_t id) noexcept;

 private:
  // ── Pipelined write group ─────────────────────────────────────────────────
  struct Writer {
    const WalEntry&               entry;
    std::expected<void, WalError> result{};
    bool                          done{false};
    std::mutex                    mu{};
    std::condition_variable       cv{};
  };

  [[nodiscard]] std::expected<void, WalError> log_pipelined(const WalEntry& entry);
  [[nodiscard]] std::expected<void, WalError> log_unordered(const WalEntry& entry);
  [[nodiscard]] std::expected<void, WalError> write_group(std::span<Writer*> group);

  // ── Block I/O ─────────────────────────────────────────────────────────────
  [[nodiscard]] std::expected<void, WalError> write_entry_blocks(const WalEntry& entry);
  [[nodiscard]] std::expected<void, WalError> write_physical_record(
      std::span<const uint8_t> payload, uint8_t type);

  // ── Encode / decode ───────────────────────────────────────────────────────
  [[nodiscard]] static std::vector<uint8_t>              encode_payload(const WalEntry& e);
  [[nodiscard]] static std::expected<WalEntry, WalError> decode_payload(
      std::span<const uint8_t> raw);

  // ── File management ───────────────────────────────────────────────────────
  void cleaner();
  void cleanWALFile();
  void reset_file();

  // Helper used by cleanWALFile: returns all tranc_ids stored in one file.
  [[nodiscard]] static std::vector<uint64_t> scan_tranc_ids(FileObj& file) noexcept;

  // ── State ─────────────────────────────────────────────────────────────────
  std::string active_log_path_;
  FileObj     log_file_;
  size_t      file_size_limit_;
  size_t      block_offset_{0};  // byte offset within the current 32 KB block

  // Serialises write-group leaders (one at a time).
  std::mutex write_mutex_;
  // Guards writer_queue_.
  std::mutex          queue_mutex_;
  std::deque<Writer*> writer_queue_;

  uint64_t   checkpoint_tranc_id_;
  std::mutex cp_mutex_;

  std::thread             cleaner_thread_;
  std::mutex              cleaner_mutex_;
  std::condition_variable cleaner_cv_;
  bool                    stop_cleaner_{false};
  uint64_t                clean_interval_s_;
};