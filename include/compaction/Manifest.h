#pragma once
#include "../storage/file.h"
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

// ─── SST Metadata ────────────────────────────────────────────────────────────
//  Mirrors the range information stored in MANIFEST for one SST file.
struct SstMeta {
  size_t      sst_id      {0};
  size_t      level       {0};
  uint64_t    min_tranc_id{0};
  uint64_t    max_tranc_id{0};
  std::string first_key;
  std::string last_key;
};

// ─── Manifest ────────────────────────────────────────────────────────────────
//
//  Append-only binary log that tracks which SST files are "live".
//  Inspired by LevelDB's MANIFEST / CURRENT pattern.
//
//  Record wire format (all little-endian):
//    type    : uint8    record type
//    len     : uint32   payload length in bytes
//    payload : len      see below
//    crc     : uint32   CRC-32C of (type ‖ payload), unfused
//
//  ADD_SST  (0x01) payload  — registers a new live SST:
//    sst_id(8) | level(8) | min_tranc_id(8) | max_tranc_id(8)
//    | first_key_len(4) | first_key | last_key_len(4) | last_key
//
//  REMOVE_SST (0x02) payload — marks an SST as compacted / removed:
//    sst_id(8)
//
//  Durability guarantee:
//    Every write_record() call appends then fsyncs, so a crash after
//    add_sst / remove_sst returns always leaves a complete, valid record.
//
//  Crash safety for compaction:
//    ADD_SST records for new SSTs are written *before* REMOVE_SST records
//    for old ones.  On crash recovery both old and new SSTs exist on disk,
//    and replaying the MANIFEST correctly reflects whichever records
//    survived.  Duplicate SSTs after a crash are safely handled by the
//    engine's compaction logic on the next startup.
// ─────────────────────────────────────────────────────────────────────────────
class Manifest {
 public:
  explicit Manifest(std::string_view dir);

  Manifest(const Manifest&)            = delete;
  Manifest& operator=(const Manifest&) = delete;
  Manifest(Manifest&&)                 = delete;
  Manifest& operator=(Manifest&&)      = delete;

  // Write ADD_SST record + fsync.  Thread-safe.
  void add_sst(const SstMeta& meta);
  // Write REMOVE_SST record + fsync.  Thread-safe.
  void remove_sst(size_t sst_id);
  // Truncate MANIFEST and clear the in-memory live-set.  Thread-safe.
  void clear();

  // Snapshot of all currently live SST metadata.
  [[nodiscard]] std::vector<SstMeta> get_live_ssts() const;
  // max(max_tranc_id) over all live SSTs; 0 when no SSTs exist.
  [[nodiscard]] uint64_t checkpoint_tranc_id() const;
  // True when an existing MANIFEST file was found and replayed on construction.
  [[nodiscard]] bool was_loaded() const noexcept { return loaded_; }

 private:
  static constexpr uint8_t kAddSst    = 0x01;
  static constexpr uint8_t kRemoveSst = 0x02;

  // Internal: replay all valid records from an open FileObj.
  void replay(FileObj& f);
  // Internal: serialise one record and fsync — caller must hold mu_.
  void write_record(uint8_t type, std::vector<uint8_t> payload);

  std::string               path_;
  FileObj                   file_;
  std::map<size_t, SstMeta> live_;  // sst_id → meta
  mutable std::mutex        mu_;
  bool                      loaded_{false};
};