#include "../../include/storage/wal.h"

#include <algorithm>
#include <array>
#include <filesystem>
#include <ranges>
#include <stdexcept>

// ─── CRC-32C (Castagnoli) ─────────────────────────────────────────────────────
namespace {

// Generate lookup table at compile time.
consteval auto make_crc32c_table() noexcept {
  std::array<uint32_t, 256> t{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k) c = (c >> 1) ^ (0x82F63B78u & -(c & 1u));
    t[i] = c;
  }
  return t;
}
inline constexpr auto kCrc32cLookup = make_crc32c_table();

[[nodiscard]] constexpr uint32_t crc32c(std::span<const uint8_t> data) noexcept {
  uint32_t crc = ~0u;
  for (auto b : data) crc = (crc >> 8) ^ kCrc32cLookup[(crc ^ b) & 0xFF];
  return ~crc;
}

// LevelDB-style masking: rotate + salt so a CRC of all-zeros differs from 0.
[[nodiscard]] constexpr uint32_t mask_crc(uint32_t crc) noexcept {
  return ((crc >> 15) | (crc << 17)) + 0xa282ead8u;
}
[[nodiscard]] constexpr uint32_t unmask_crc(uint32_t v) noexcept {
  const uint32_t rot = v - 0xa282ead8u;
  return (rot >> 17) | (rot << 15);
}

// ─── Block / record constants ─────────────────────────────────────────────────
inline constexpr size_t kBlockSize  = Global_::WAL_BLOCK_SIZE;  // 32 KB
inline constexpr size_t kHeaderSize = 7;                        // crc(4)+len(2)+type(1)

inline constexpr uint8_t kRecZero   = 0;  // padding / empty
inline constexpr uint8_t kRecFull   = 1;  // fits in one block
inline constexpr uint8_t kRecFirst  = 2;  // first fragment
inline constexpr uint8_t kRecMiddle = 3;  // interior fragment
inline constexpr uint8_t kRecLast   = 4;  // last fragment

// ─── Little-endian helpers ────────────────────────────────────────────────────
template <std::integral T>
void write_le(std::vector<uint8_t>& buf, T v) noexcept {
  for (size_t i = 0; i < sizeof(T); ++i) buf.push_back(static_cast<uint8_t>(v >> (i * 8)));
}

template <std::integral T>
[[nodiscard]] constexpr T read_le(std::span<const uint8_t> src, size_t off = 0) noexcept {
  T v{};
  for (size_t i = 0; i < sizeof(T); ++i) v |= static_cast<T>(src[off + i]) << (i * 8);
  return v;
}

}  // namespace

// ─── Constructor ─────────────────────────────────────────────────────────────

WAL::WAL(std::string_view log_dir, uint64_t checkpoint_tranc_id, uint64_t clean_interval_s,
         uint64_t file_size_limit)
    : file_size_limit_(file_size_limit),
      checkpoint_tranc_id_(checkpoint_tranc_id),
      clean_interval_s_(clean_interval_s) {
  namespace fs = std::filesystem;
  if (!fs::exists(log_dir))
    fs::create_directories(log_dir);

  active_log_path_ = std::string(log_dir) + "/wal.0";
  log_file_        = FileObj::open(active_log_path_, /*create=*/true);
  // Resume at the correct position inside the last block.
  block_offset_ = log_file_.size() % kBlockSize;

  cleaner_thread_ = std::thread(&WAL::cleaner, this);
}

// ─── Destructor ───────────────────────────────────────────────────────────────

WAL::~WAL() {
  flush();  // ensure everything is fsync'd

  {
    std::lock_guard lk(cleaner_mutex_);
    stop_cleaner_ = true;
  }
  cleaner_cv_.notify_one();
  if (cleaner_thread_.joinable())
    cleaner_thread_.join();

  log_file_.close();
}

// ─── flush ────────────────────────────────────────────────────────────────────

std::expected<void, WalError> WAL::flush() {
  std::lock_guard lk(write_mutex_);
  if (!log_file_.sync())
    return std::unexpected(WalError::kSyncFailed);
  return {};
}

// ─── set_checkpoint_tranc_id ──────────────────────────────────────────────────

void WAL::set_checkpoint_tranc_id(uint64_t id) noexcept {
  std::lock_guard lk(cp_mutex_);
  checkpoint_tranc_id_ = id;
}

// ─── log (policy dispatch) ────────────────────────────────────────────────────

std::expected<void, WalError> WAL::log(const WalEntry& entry) {
  if constexpr (Global_::WAL_WRITE_POLICY == Global_::WalWritePolicy::kPipelined)
    return log_pipelined(entry);
  else
    return log_unordered(entry);
}

// ─── log_batch ────────────────────────────────────────────────────────────────
//
//  Writes every entry in `entries` to the WAL and issues exactly one fsync,
//  regardless of WAL_WRITE_POLICY.  Holding write_mutex_ for the full batch
//  provides atomic durability: either all entries are fsynced or the call
//  returns an error.
//
//  Design notes:
//  • Bypassing pipelining is intentional – a batch is already a "group write"
//    and grouping groups adds no benefit.
//  • Concurrent single log() calls in kPipelined mode will queue behind this
//    batch's write_mutex_ hold, which is correct behaviour.
//  • On error, entries already written to the kernel buffer are NOT rolled
//    back.  Callers must treat a WAL write failure as fatal and stop accepting
//    new writes.

std::expected<void, WalError> WAL::log_batch(std::span<const WalEntry> entries) {
  if (entries.empty()) return {};

  std::lock_guard lk(write_mutex_);

  for (const auto& e : entries) {
    if (auto r = write_entry_blocks(e); !r)
      return r;
  }

  if (log_file_.size() > file_size_limit_) {
    // Rolling the file here keeps each WAL segment bounded; the cleaner will
    // eventually remove segments whose entries are all checkpointed.
    reset_file();
    // reset_file() does its own fsync on the old file before rotating.
    return {};
  }

  if (!log_file_.sync())
    return std::unexpected(WalError::kSyncFailed);

  return {};
}
// ─── Unordered path ───────────────────────────────────────────────────────────
// Each caller independently serialises through write_mutex_.
// No grouping; lock is held only for one entry's write + fsync.

std::expected<void, WalError> WAL::log_unordered(const WalEntry& entry) {
  std::lock_guard lk(write_mutex_);
  if (auto r = write_entry_blocks(entry); !r)
    return r;
  if (log_file_.size() > file_size_limit_)
    reset_file();
  if (!log_file_.sync())
    return std::unexpected(WalError::kSyncFailed);
  return {};
}

// ─── Pipelined path ───────────────────────────────────────────────────────────
//
//  Timeline (T1 = leader, T2/T3 = followers):
//
//   T1: enqueue → try_lock(write_mutex_) succeeds
//       → drain queue (may collect T2 if T2 already enqueued)
//       → write WAL for whole group
//       → fsync
//       → signal all members (sets done=true on their Writers)
//       → release write_mutex_           ← T3 can start its WAL write HERE
//   T1: sees done=true on its own Writer, returns
//   T1: caller writes MemTable           ← parallel with T3's WAL write
//
//  Followers wait on their Writer::cv; they never hold write_mutex_.

std::expected<void, WalError> WAL::log_pipelined(const WalEntry& entry) {
  Writer w{.entry = entry};

  {
    std::lock_guard qlk(queue_mutex_);
    writer_queue_.push_back(&w);
  }

  if (std::unique_lock wlk(write_mutex_, std::try_to_lock); wlk.owns_lock()) {
    // ── Leader: drain queue and write the whole batch ─────────────────────
    std::vector<Writer*> group;
    {
      std::lock_guard qlk(queue_mutex_);
      group.assign(writer_queue_.begin(), writer_queue_.end());
      writer_queue_.clear();
    }

    const auto res = write_group(std::span{group});

    // Signal every member *before* releasing write_mutex_ so that no
    // member can observe a partially-written group.
    for (auto* gw : group) {
      {
        std::lock_guard glk(gw->mu);
        gw->result = res;
        gw->done   = true;
      }
      gw->cv.notify_one();
    }
    // wlk destroyed here → write_mutex_ released → next leader unblocks.
  }

  // Both leader and followers converge here and wait on their own cv.
  // The leader's Writer was already signalled above, so it returns immediately.
  std::unique_lock lk(w.mu);
  w.cv.wait(lk, [&w] { return w.done; });
  return w.result;
}

// ─── write_group ─────────────────────────────────────────────────────────────

std::expected<void, WalError> WAL::write_group(std::span<Writer*> group) {
  for (auto* w : group) {
    if (auto r = write_entry_blocks(w->entry); !r)
      return r;
  }
  if (log_file_.size() > file_size_limit_)
    reset_file();
  if (!log_file_.sync())
    return std::unexpected(WalError::kSyncFailed);
  return {};
}

// ─── Block-level I/O ─────────────────────────────────────────────────────────

std::expected<void, WalError> WAL::write_entry_blocks(const WalEntry& entry) {
  const auto               payload = encode_payload(entry);
  std::span<const uint8_t> remaining{payload};
  bool                     is_first = true;

  while (!remaining.empty()) {
    size_t avail = kBlockSize - block_offset_;

    if (avail < kHeaderSize) {
      // Pad the tail of the current block with zeros so that recovery
      // can unambiguously detect block boundaries.
      std::vector<uint8_t> pad(avail, kRecZero);
      if (!log_file_.append(pad))
        return std::unexpected(WalError::kIOError);
      block_offset_ = 0;
      avail         = kBlockSize;
    }

    const size_t data_avail = avail - kHeaderSize;
    const size_t chunk_len  = std::min(data_avail, remaining.size());
    const bool   is_last    = (chunk_len == remaining.size());

    const uint8_t type = (is_first && is_last) ? kRecFull
                         : is_first            ? kRecFirst
                         : is_last             ? kRecLast
                                               : kRecMiddle;

    if (auto r = write_physical_record(remaining.subspan(0, chunk_len), type); !r)
      return r;

    remaining = remaining.subspan(chunk_len);
    is_first  = false;
  }
  return {};
}

std::expected<void, WalError> WAL::write_physical_record(std::span<const uint8_t> data,
                                                         uint8_t                  type) {
  // CRC-32C covers [type || data]
  std::vector<uint8_t> crc_input;
  crc_input.reserve(1 + data.size());
  crc_input.push_back(type);
  crc_input.insert(crc_input.end(), data.begin(), data.end());

  const uint32_t crc = mask_crc(crc32c(std::span{crc_input}));
  const uint16_t len = static_cast<uint16_t>(data.size());

  std::vector<uint8_t> record;
  record.reserve(kHeaderSize + data.size());
  write_le(record, crc);
  write_le(record, len);
  record.push_back(type);
  record.insert(record.end(), data.begin(), data.end());

  if (!log_file_.append(record))
    return std::unexpected(WalError::kIOError);

  block_offset_ += kHeaderSize + data.size();
  if (block_offset_ == kBlockSize)
    block_offset_ = 0;

  return {};
}

// ─── encode_payload / decode_payload ─────────────────────────────────────────

std::vector<uint8_t> WAL::encode_payload(const WalEntry& e) {
  std::vector<uint8_t> buf;
  buf.reserve(4 + e.key.size() + 4 + e.value.size() + 8);
  write_le<uint32_t>(buf, static_cast<uint32_t>(e.key.size()));
  buf.insert(buf.end(), e.key.begin(), e.key.end());
  write_le<uint32_t>(buf, static_cast<uint32_t>(e.value.size()));
  buf.insert(buf.end(), e.value.begin(), e.value.end());
  write_le<uint64_t>(buf, e.tranc_id);
  return buf;
}

std::expected<WalEntry, WalError> WAL::decode_payload(std::span<const uint8_t> raw) {
  // Minimum: key_len(4) + value_len(4) + tranc_id(8) = 16 bytes
  if (raw.size() < 16)
    return std::unexpected(WalError::kCorrupted);

  size_t off = 0;

  const auto key_len = read_le<uint32_t>(raw, off);
  off += 4;
  if (off + key_len > raw.size())
    return std::unexpected(WalError::kCorrupted);
  std::string key(reinterpret_cast<const char*>(raw.data() + off), key_len);
  off += key_len;

  if (off + 4 > raw.size())
    return std::unexpected(WalError::kCorrupted);
  const auto val_len = read_le<uint32_t>(raw, off);
  off += 4;
  if (off + val_len > raw.size())
    return std::unexpected(WalError::kCorrupted);
  std::string value(reinterpret_cast<const char*>(raw.data() + off), val_len);
  off += val_len;

  if (off + 8 > raw.size())
    return std::unexpected(WalError::kCorrupted);
  const auto tranc_id = read_le<uint64_t>(raw, off);

  return WalEntry{std::move(key), std::move(value), tranc_id};
}

// ─── recover ─────────────────────────────────────────────────────────────────
//
//  Reads all wal.N files in ascending sequence order.
//  Stops (not errors) on a truncated record – this is the expected last-write
//  scenario.  Returns kChecksumMismatch on a header-complete but corrupt
//  record to allow the caller to make a recovery-mode decision (analogous to
//  RocksDB issue #6963).

std::expected<std::map<uint64_t, std::vector<WalEntry>>, WalError> WAL::recover(
    std::string_view log_dir, uint64_t checkpoint_tranc_id) {
  namespace fs = std::filesystem;

  std::map<uint64_t, std::vector<WalEntry>> result;
  if (!fs::exists(log_dir))
    return result;

  // Collect and sort by sequence number
  std::vector<std::pair<uint64_t, std::string>> wal_files;
  for (const auto& de : fs::directory_iterator(log_dir)) {
    if (!de.is_regular_file())
      continue;
    const auto name = de.path().filename().string();
    if (!name.starts_with("wal."))
      continue;
    try {
      wal_files.emplace_back(std::stoull(name.substr(4)), de.path().string());
    } catch (...) {
    }
  }
  std::ranges::sort(wal_files);

  for (const auto& [unused_sq, path] : wal_files) {
    auto                 file      = FileObj::open(path, /*create=*/false);
    size_t               file_size = file.size();
    size_t               offset    = 0;
    std::vector<uint8_t> scratch;  // accumulates record fragments

    while (offset < file_size) {
      // Skip block-tail padding (< kHeaderSize bytes left in this block)
      const size_t block_off = offset % kBlockSize;
      if (kBlockSize - block_off < kHeaderSize) {
        offset += kBlockSize - block_off;
        continue;
      }
      if (offset + kHeaderSize > file_size)
        break;  // truncated header → stop

      const uint32_t stored_crc = unmask_crc(file.read_uint32(offset));
      const uint16_t rec_len    = file.read_uint16(offset + 4);
      const uint8_t  rec_type   = file.read_uint8(offset + 6);

      if (offset + kHeaderSize + rec_len > file_size) {
        // Partial data – clean truncation.  Per #12488 we stop here
        // rather than silently accepting a zero-length tail.
        break;
      }

      // Verify checksum
      {
        auto                 data = file.read_to_slice(offset + kHeaderSize, rec_len);
        std::vector<uint8_t> check;
        check.reserve(1 + rec_len);
        check.push_back(rec_type);
        check.insert(check.end(), data.begin(), data.end());
        if (crc32c(std::span{check}) != stored_crc)
          return std::unexpected(WalError::kChecksumMismatch);
      }

      auto data = file.read_to_slice(offset + kHeaderSize, rec_len);
      offset += kHeaderSize + rec_len;

      bool complete = false;
      switch (rec_type) {
        case kRecZero:
          // Padding – skip the rest of the block
          offset += kBlockSize - (offset % kBlockSize);
          continue;
        case kRecFull:
          scratch  = std::move(data);
          complete = true;
          break;
        case kRecFirst:
          scratch = std::move(data);
          break;
        case kRecMiddle:
          scratch.insert(scratch.end(), data.begin(), data.end());
          break;
        case kRecLast:
          scratch.insert(scratch.end(), data.begin(), data.end());
          complete = true;
          break;
        default:
          return std::unexpected(WalError::kCorrupted);
      }

      if (complete) {
        auto entry_res = decode_payload(std::span{scratch});
        if (!entry_res)
          return std::unexpected(entry_res.error());
        auto& e = *entry_res;
        if (e.tranc_id > checkpoint_tranc_id)
          result[e.tranc_id].push_back(std::move(e));
        scratch.clear();
      }
    }
  }
  return result;
}

// ─── scan_tranc_ids (cleaner helper) ─────────────────────────────────────────
// Light version of recover: no CRC check, just collects all tranc_ids.

std::vector<uint64_t> WAL::scan_tranc_ids(FileObj& file) noexcept {
  std::vector<uint64_t> ids;
  const size_t          file_size = file.size();
  size_t                offset    = 0;
  std::vector<uint8_t>  scratch;

  try {
    while (offset < file_size) {
      const size_t block_off = offset % kBlockSize;
      if (kBlockSize - block_off < kHeaderSize) {
        offset += kBlockSize - block_off;
        continue;
      }
      if (offset + kHeaderSize > file_size)
        break;

      const uint16_t rec_len  = file.read_uint16(offset + 4);
      const uint8_t  rec_type = file.read_uint8(offset + 6);

      if (offset + kHeaderSize + rec_len > file_size)
        break;

      auto data = file.read_to_slice(offset + kHeaderSize, rec_len);
      offset += kHeaderSize + rec_len;

      bool complete = false;
      switch (rec_type) {
        case kRecZero:
          offset += kBlockSize - (offset % kBlockSize);
          continue;
        case kRecFull:
          scratch  = std::move(data);
          complete = true;
          break;
        case kRecFirst:
          scratch = std::move(data);
          break;
        case kRecMiddle:
          scratch.insert(scratch.end(), data.begin(), data.end());
          break;
        case kRecLast:
          scratch.insert(scratch.end(), data.begin(), data.end());
          complete = true;
          break;
        default:
          scratch.clear();
          break;
      }

      if (complete && scratch.size() >= 16) {
        const auto   key_len = read_le<uint32_t>(std::span{scratch}, 0);
        const size_t val_off = 4 + key_len + 4;
        if (val_off <= scratch.size()) {
          const auto   val_len = read_le<uint32_t>(std::span{scratch}, 4 + key_len);
          const size_t tid_off = val_off + val_len;
          if (tid_off + 8 <= scratch.size())
            ids.push_back(read_le<uint64_t>(std::span{scratch}, tid_off));
        }
        scratch.clear();
      }
    }
  } catch (...) {
  }  // tolerate I/O errors during cleanup

  return ids;
}

// ─── reset_file ──────────────────────────────────────────────────────────────

void WAL::reset_file() {
  const auto dot   = active_log_path_.find_last_of('.');
  const auto seq   = std::stoul(active_log_path_.substr(dot + 1)) + 1;
  active_log_path_ = active_log_path_.substr(0, dot + 1) + std::to_string(seq);
  log_file_        = FileObj::create_and_write(active_log_path_, {});
  block_offset_    = 0;
}

// ─── cleaner thread ───────────────────────────────────────────────────────────

void WAL::cleaner() {
  while (true) {
    std::unique_lock lk(cleaner_mutex_);
    cleaner_cv_.wait_for(lk, std::chrono::seconds(clean_interval_s_),
                         [this] { return stop_cleaner_; });
    if (stop_cleaner_)
      break;
    lk.unlock();
    cleanWALFile();
  }
}

void WAL::cleanWALFile() {
  namespace fs = std::filesystem;

  // Capture active path and checkpoint under their respective locks.
  std::string dir_path;
  {
    std::lock_guard wlk(write_mutex_);
    const auto      slash = active_log_path_.find_last_of('/');
    dir_path = (slash != std::string::npos) ? active_log_path_.substr(0, slash + 1) : "./";
  }
  uint64_t cp;
  {
    std::lock_guard clk(cp_mutex_);
    cp = checkpoint_tranc_id_;
  }

  std::vector<std::pair<uint64_t, std::string>> wal_files;
  for (const auto& de : fs::directory_iterator(dir_path)) {
    if (!de.is_regular_file())
      continue;
    const auto name = de.path().filename().string();
    if (!name.starts_with("wal."))
      continue;
    try {
      wal_files.emplace_back(std::stoull(name.substr(4)), de.path().string());
    } catch (...) {
    }
  }
  std::ranges::sort(wal_files);

  // Never touch the last (active) file.
  if (wal_files.size() <= 1)
    return;

  for (size_t i = 0; i + 1 < wal_files.size(); ++i) {
    auto       file = FileObj::open(wal_files[i].second, /*create=*/false);
    const auto ids  = scan_tranc_ids(file);

    // Safe to delete only when every recorded tranc_id has been checkpointed.
    const bool all_committed =
        !ids.empty() && std::ranges::all_of(ids, [cp](uint64_t id) { return id <= cp; });

    if (all_committed)
      file.del_file();
  }
}