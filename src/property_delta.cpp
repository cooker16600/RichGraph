#include "core/property_delta.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace lsmgraph {
namespace {

constexpr std::array<char, 8> kMagic{{'R', 'G', 'D', 'E', 'L', 'T', 'A', '2'}};
constexpr std::uint32_t kFormatVersion = 2;

#pragma pack(push, 1)
struct DeltaDiskHeader {
  char magic[8];
  std::uint32_t version;
  std::uint32_t header_bytes;
  std::uint8_t object_kind;
  std::uint8_t reserved0[7];
  std::uint32_t shard_id;
  std::uint32_t property_id;
  std::uint32_t base_file_id;
  std::uint32_t reserved1;
  std::uint64_t base_generation;
  std::uint64_t file_generation;
  std::uint64_t record_count;
  std::uint64_t records_offset;
  std::uint64_t values_offset;
  std::uint64_t values_bytes;
  std::uint64_t min_commit_sequence;
  std::uint64_t max_commit_sequence;
  std::uint64_t payload_checksum;
  std::uint64_t header_checksum;
};

struct DeltaDiskRecord {
  std::uint64_t src;
  std::uint64_t dst;
  std::int64_t base_sequence;
  std::uint64_t commit_sequence;
  std::uint64_t value_offset;
  std::uint32_t value_length;
  std::uint8_t edge_type;
  std::uint8_t is_out;
  std::uint16_t reserved;
};
#pragma pack(pop)

static_assert(sizeof(DeltaDiskHeader) == 120,
              "unexpected property-delta header layout");
static_assert(sizeof(DeltaDiskRecord) == 48,
              "unexpected property-delta record layout");

std::string ErrnoMessage(std::string_view operation,
                         const std::string& path) {
  return std::string(operation) + " " + path + ": " + std::strerror(errno);
}

void SetError(std::string* error, std::string message) {
  if (error != nullptr) {
    *error = std::move(message);
  }
}

// FNV-1a is used as an integrity checksum, not as a cryptographic primitive.
std::uint64_t ExtendChecksum(std::uint64_t hash,
                             const void* data,
                             std::size_t size) {
  constexpr std::uint64_t kPrime = 1099511628211ULL;
  const auto* bytes = static_cast<const unsigned char*>(data);
  for (std::size_t i = 0; i < size; ++i) {
    hash ^= bytes[i];
    hash *= kPrime;
  }
  return hash;
}

std::uint64_t Checksum(const void* data, std::size_t size) {
  constexpr std::uint64_t kOffset = 14695981039346656037ULL;
  return ExtendChecksum(kOffset, data, size);
}

std::uint64_t HeaderChecksum(DeltaDiskHeader header) {
  header.header_checksum = 0;
  return Checksum(&header, sizeof(header));
}

bool WriteAll(int fd,
              const void* data,
              std::size_t size,
              const std::string& path,
              std::string* error) {
  const auto* cursor = static_cast<const std::byte*>(data);
  std::size_t remaining = size;
  while (remaining != 0) {
    const ssize_t written = ::write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      SetError(error, ErrnoMessage("write", path));
      return false;
    }
    if (written == 0) {
      SetError(error, "short write " + path);
      return false;
    }
    cursor += written;
    remaining -= static_cast<std::size_t>(written);
  }
  return true;
}

bool SyncParentDirectory(const std::string& path, std::string* error) {
  const std::filesystem::path parent =
      std::filesystem::path(path).parent_path();
  const std::string directory = parent.empty() ? "." : parent.string();
  const int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) {
    SetError(error, ErrnoMessage("open directory", directory));
    return false;
  }
  const bool ok = ::fsync(fd) == 0;
  if (!ok) {
    SetError(error, ErrnoMessage("fsync directory", directory));
  }
  ::close(fd);
  return ok;
}

int CompareLogicalKey(const PropertyDeltaRecord& lhs,
                      const PropertyDeltaRecord& rhs) {
  if (lhs.src != rhs.src) return lhs.src < rhs.src ? -1 : 1;
  if (lhs.dst != rhs.dst) return lhs.dst < rhs.dst ? -1 : 1;
  if (lhs.is_out != rhs.is_out) return lhs.is_out ? 1 : -1;
  if (lhs.edge_type != rhs.edge_type) {
    return lhs.edge_type < rhs.edge_type ? -1 : 1;
  }
  if (lhs.base_sequence != rhs.base_sequence) {
    return lhs.base_sequence < rhs.base_sequence ? -1 : 1;
  }
  return 0;
}

bool RecordLess(const PropertyDeltaRecord& lhs,
                const PropertyDeltaRecord& rhs) {
  const int key_comparison = CompareLogicalKey(lhs, rhs);
  if (key_comparison != 0) return key_comparison < 0;
  return lhs.commit_sequence < rhs.commit_sequence;
}

bool SameVersion(const PropertyDeltaRecord& lhs,
                 const PropertyDeltaRecord& rhs) {
  return CompareLogicalKey(lhs, rhs) == 0 &&
         lhs.commit_sequence == rhs.commit_sequence;
}

PropertyDeltaRecord DiskRecordToRecord(const DeltaDiskRecord& disk,
                                       const std::byte* mapping,
                                       std::uint64_t values_offset) {
  PropertyDeltaRecord record;
  record.src = disk.src;
  record.dst = disk.dst;
  record.base_sequence = disk.base_sequence;
  record.commit_sequence = disk.commit_sequence;
  record.is_out = disk.is_out != 0;
  record.edge_type = disk.edge_type;
  record.value.assign(
      reinterpret_cast<const char*>(mapping + values_offset +
                                    disk.value_offset),
      disk.value_length);
  return record;
}

int CompareDiskLogicalKey(const DeltaDiskRecord& record,
                          VertexId_t src,
                          VertexId_t dst,
                          SequenceNumber_t base_sequence,
                          bool is_out,
                          std::uint8_t edge_type) {
  if (record.src != src) return record.src < src ? -1 : 1;
  if (record.dst != dst) return record.dst < dst ? -1 : 1;
  const std::uint8_t direction = is_out ? 1U : 0U;
  if (record.is_out != direction) {
    return record.is_out < direction ? -1 : 1;
  }
  if (record.edge_type != edge_type) {
    return record.edge_type < edge_type ? -1 : 1;
  }
  if (record.base_sequence != base_sequence) {
    return record.base_sequence < base_sequence ? -1 : 1;
  }
  return 0;
}

DeltaDiskRecord LoadDiskRecord(const std::byte* mapping,
                               std::uint64_t records_offset,
                               std::uint64_t index) {
  DeltaDiskRecord record{};
  std::memcpy(&record,
              mapping + records_offset + index * sizeof(DeltaDiskRecord),
              sizeof(record));
  return record;
}

bool IsValidObjectKind(std::uint8_t raw_kind) {
  return raw_kind == static_cast<std::uint8_t>(PropertyObjectKind::kNode) ||
         raw_kind == static_cast<std::uint8_t>(PropertyObjectKind::kEdge);
}

}  // namespace

bool PropertyDeltaTarget::operator==(
    const PropertyDeltaTarget& other) const {
  return kind == other.kind && shard_id == other.shard_id &&
         base_file_id == other.base_file_id &&
         base_generation == other.base_generation &&
         property_id == other.property_id;
}

std::size_t PropertyDeltaTargetHash::operator()(
    const PropertyDeltaTarget& target) const {
  std::size_t hash = std::hash<std::uint64_t>{}(target.base_generation);
  const auto combine = [&hash](std::size_t value) {
    hash ^= value + 0x9e3779b97f4a7c15ULL + (hash << 6U) + (hash >> 2U);
  };
  combine(std::hash<std::uint8_t>{}(static_cast<std::uint8_t>(target.kind)));
  combine(std::hash<std::uint32_t>{}(target.shard_id));
  combine(std::hash<FileId_t>{}(target.base_file_id));
  combine(std::hash<std::uint32_t>{}(target.property_id));
  return hash;
}

std::shared_ptr<PropertyDeltaFile> PropertyDeltaFile::Create(
    const std::string& path,
    const PropertyDeltaTarget& target,
    std::uint64_t file_generation,
    std::vector<PropertyDeltaRecord> records,
    DeltaDurability durability,
    std::string* error) {
  if (records.empty()) {
    SetError(error, "cannot create an empty property delta");
    return nullptr;
  }
  if (target.base_file_id == INVALID_File_ID) {
    SetError(error, "property delta target has an invalid file id");
    return nullptr;
  }

  std::stable_sort(records.begin(), records.end(), RecordLess);
  std::vector<PropertyDeltaRecord> deduplicated;
  deduplicated.reserve(records.size());
  for (auto& record : records) {
    if (!deduplicated.empty() && SameVersion(deduplicated.back(), record)) {
      deduplicated.back() = std::move(record);
    } else {
      deduplicated.push_back(std::move(record));
    }
  }

  std::vector<DeltaDiskRecord> disk_records;
  disk_records.reserve(deduplicated.size());
  std::vector<std::byte> values;
  PropertyCommitSequence min_commit = kLatestPropertyCommit;
  PropertyCommitSequence max_commit = 0;
  for (const auto& record : deduplicated) {
    if (record.value.size() > std::numeric_limits<std::uint32_t>::max()) {
      SetError(error, "property delta value exceeds uint32 length");
      return nullptr;
    }
    if (values.size() > std::numeric_limits<std::uint64_t>::max() -
                            record.value.size()) {
      SetError(error, "property delta values overflow uint64 length");
      return nullptr;
    }
    DeltaDiskRecord disk{};
    disk.src = record.src;
    disk.dst = record.dst;
    disk.base_sequence = record.base_sequence;
    disk.commit_sequence = record.commit_sequence;
    disk.value_offset = values.size();
    disk.value_length = static_cast<std::uint32_t>(record.value.size());
    disk.edge_type = record.edge_type;
    disk.is_out = record.is_out ? 1U : 0U;
    disk_records.push_back(disk);
    const auto* begin = reinterpret_cast<const std::byte*>(record.value.data());
    values.insert(values.end(), begin, begin + record.value.size());
    min_commit = std::min(min_commit, record.commit_sequence);
    max_commit = std::max(max_commit, record.commit_sequence);
  }

  DeltaDiskHeader header{};
  std::memcpy(header.magic, kMagic.data(), kMagic.size());
  header.version = kFormatVersion;
  header.header_bytes = sizeof(header);
  header.object_kind = static_cast<std::uint8_t>(target.kind);
  header.shard_id = target.shard_id;
  header.property_id = target.property_id;
  header.base_file_id = target.base_file_id;
  header.base_generation = target.base_generation;
  header.file_generation = file_generation;
  header.record_count = disk_records.size();
  header.records_offset = sizeof(header);
  header.values_offset = header.records_offset +
                         disk_records.size() * sizeof(DeltaDiskRecord);
  header.values_bytes = values.size();
  header.min_commit_sequence = min_commit;
  header.max_commit_sequence = max_commit;
  constexpr std::uint64_t kOffset = 14695981039346656037ULL;
  std::uint64_t payload_checksum = kOffset;
  if (!disk_records.empty()) {
    payload_checksum = ExtendChecksum(payload_checksum,
                                      disk_records.data(),
                                      disk_records.size() *
                                          sizeof(DeltaDiskRecord));
  }
  if (!values.empty()) {
    payload_checksum = ExtendChecksum(payload_checksum,
                                      values.data(),
                                      values.size());
  }
  header.payload_checksum = payload_checksum;
  header.header_checksum = HeaderChecksum(header);

  const std::string temporary = path + ".tmp." +
                                std::to_string(::getpid()) + "." +
                                std::to_string(file_generation);
  const int fd = ::open(temporary.c_str(),
                        O_CREAT | O_EXCL | O_WRONLY,
                        0644);
  if (fd < 0) {
    SetError(error, ErrnoMessage("open", temporary));
    return nullptr;
  }

  bool ok = WriteAll(fd, &header, sizeof(header), temporary, error) &&
            WriteAll(fd,
                     disk_records.data(),
                     disk_records.size() * sizeof(DeltaDiskRecord),
                     temporary,
                     error) &&
            WriteAll(fd, values.data(), values.size(), temporary, error);
  if (ok && durability == DeltaDurability::kProcessCrashSafe &&
      ::fdatasync(fd) != 0) {
    SetError(error, ErrnoMessage("fdatasync", temporary));
    ok = false;
  }
  if (::close(fd) != 0 && ok) {
    SetError(error, ErrnoMessage("close", temporary));
    ok = false;
  }
  if (!ok) {
    ::unlink(temporary.c_str());
    return nullptr;
  }
  if (::rename(temporary.c_str(), path.c_str()) != 0) {
    SetError(error, ErrnoMessage("rename", path));
    ::unlink(temporary.c_str());
    return nullptr;
  }
  if (durability == DeltaDurability::kProcessCrashSafe &&
      !SyncParentDirectory(path, error)) {
    return nullptr;
  }
  return Open(path, error);
}

std::shared_ptr<PropertyDeltaFile> PropertyDeltaFile::Open(
    const std::string& path,
    std::string* error) {
  auto file = std::shared_ptr<PropertyDeltaFile>(new PropertyDeltaFile());
  if (!file->OpenAndValidate(path, error)) {
    return nullptr;
  }
  return file;
}

bool PropertyDeltaFile::OpenAndValidate(const std::string& path,
                                        std::string* error) {
  path_ = path;
  fd_ = ::open(path.c_str(), O_RDONLY);
  if (fd_ < 0) {
    SetError(error, ErrnoMessage("open", path));
    return false;
  }
  struct stat file_status {};
  if (::fstat(fd_, &file_status) != 0 || file_status.st_size < 0) {
    SetError(error, ErrnoMessage("fstat", path));
    return false;
  }
  mapping_size_ = static_cast<std::size_t>(file_status.st_size);
  if (mapping_size_ < sizeof(DeltaDiskHeader)) {
    SetError(error, "property delta is shorter than its header: " + path);
    return false;
  }
  void* mapping =
      ::mmap(nullptr, mapping_size_, PROT_READ, MAP_PRIVATE, fd_, 0);
  if (mapping == MAP_FAILED) {
    SetError(error, ErrnoMessage("mmap", path));
    return false;
  }
  mapping_ = static_cast<const std::byte*>(mapping);

  DeltaDiskHeader header{};
  std::memcpy(&header, mapping_, sizeof(header));
  if (std::memcmp(header.magic, kMagic.data(), kMagic.size()) != 0 ||
      header.version != kFormatVersion ||
      header.header_bytes != sizeof(DeltaDiskHeader) ||
      !IsValidObjectKind(header.object_kind)) {
    SetError(error, "invalid property delta header: " + path);
    return false;
  }
  if (HeaderChecksum(header) != header.header_checksum) {
    SetError(error, "property delta header checksum mismatch: " + path);
    return false;
  }
  if (header.record_count >
      (std::numeric_limits<std::uint64_t>::max() - header.records_offset) /
          sizeof(DeltaDiskRecord)) {
    SetError(error, "property delta record bounds overflow: " + path);
    return false;
  }
  const std::uint64_t records_end =
      header.records_offset + header.record_count * sizeof(DeltaDiskRecord);
  if (header.values_bytes >
      std::numeric_limits<std::uint64_t>::max() - header.values_offset) {
    SetError(error, "property delta value bounds overflow: " + path);
    return false;
  }
  const std::uint64_t values_end =
      header.values_offset + header.values_bytes;
  if (header.records_offset != sizeof(DeltaDiskHeader) ||
      header.values_offset != records_end ||
      records_end > mapping_size_ || values_end != mapping_size_) {
    SetError(error, "invalid property delta bounds: " + path);
    return false;
  }
  constexpr std::uint64_t kOffset = 14695981039346656037ULL;
  std::uint64_t payload_checksum = kOffset;
  if (mapping_size_ > header.records_offset) {
    payload_checksum = ExtendChecksum(
        payload_checksum,
        mapping_ + header.records_offset,
        mapping_size_ - static_cast<std::size_t>(header.records_offset));
  }
  if (payload_checksum != header.payload_checksum) {
    SetError(error, "property delta payload checksum mismatch: " + path);
    return false;
  }

  for (std::uint64_t i = 0; i < header.record_count; ++i) {
    const DeltaDiskRecord record =
        LoadDiskRecord(mapping_, header.records_offset, i);
    if (record.value_offset > header.values_bytes ||
        record.value_length > header.values_bytes - record.value_offset) {
      SetError(error, "property delta record value is out of bounds: " + path);
      return false;
    }
    if (i != 0) {
      const DeltaDiskRecord previous =
          LoadDiskRecord(mapping_, header.records_offset, i - 1);
      PropertyDeltaRecord lhs;
      lhs.src = previous.src;
      lhs.dst = previous.dst;
      lhs.base_sequence = previous.base_sequence;
      lhs.commit_sequence = previous.commit_sequence;
      lhs.is_out = previous.is_out != 0;
      lhs.edge_type = previous.edge_type;
      PropertyDeltaRecord rhs;
      rhs.src = record.src;
      rhs.dst = record.dst;
      rhs.base_sequence = record.base_sequence;
      rhs.commit_sequence = record.commit_sequence;
      rhs.is_out = record.is_out != 0;
      rhs.edge_type = record.edge_type;
      if (RecordLess(rhs, lhs)) {
        SetError(error, "property delta records are not sorted: " + path);
        return false;
      }
    }
  }

  metadata_.target.kind =
      static_cast<PropertyObjectKind>(header.object_kind);
  metadata_.target.shard_id = header.shard_id;
  metadata_.target.property_id = header.property_id;
  metadata_.target.base_file_id = header.base_file_id;
  metadata_.target.base_generation = header.base_generation;
  metadata_.file_generation = header.file_generation;
  metadata_.record_count = header.record_count;
  metadata_.file_bytes = mapping_size_;
  metadata_.min_commit_sequence = header.min_commit_sequence;
  metadata_.max_commit_sequence = header.max_commit_sequence;
  metadata_.payload_checksum = header.payload_checksum;
  records_offset_ = header.records_offset;
  values_offset_ = header.values_offset;
  ::madvise(const_cast<std::byte*>(mapping_), mapping_size_, MADV_RANDOM);
  return true;
}

PropertyDeltaFile::~PropertyDeltaFile() {
  if (mapping_ != nullptr) {
    ::munmap(const_cast<std::byte*>(mapping_), mapping_size_);
    mapping_ = nullptr;
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  if (obsolete_.load(std::memory_order_acquire) && !path_.empty()) {
    ::unlink(path_.c_str());
  }
}

bool PropertyDeltaFile::Lookup(
    VertexId_t src,
    VertexId_t dst,
    SequenceNumber_t base_sequence,
    bool is_out,
    std::uint8_t edge_type,
    PropertyCommitSequence read_sequence,
    std::string* value,
    PropertyCommitSequence* found_commit) const {
  if (value == nullptr || mapping_ == nullptr ||
      metadata_.record_count == 0 ||
      metadata_.min_commit_sequence > read_sequence) {
    return false;
  }

  std::uint64_t low = 0;
  std::uint64_t high = metadata_.record_count;
  while (low < high) {
    const std::uint64_t middle = low + (high - low) / 2;
    const DeltaDiskRecord record =
        LoadDiskRecord(mapping_, records_offset_, middle);
    if (CompareDiskLogicalKey(record,
                              src,
                              dst,
                              base_sequence,
                              is_out,
                              edge_type) < 0) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  const std::uint64_t first = low;
  if (first == metadata_.record_count) return false;
  DeltaDiskRecord first_record =
      LoadDiskRecord(mapping_, records_offset_, first);
  if (CompareDiskLogicalKey(first_record,
                            src,
                            dst,
                            base_sequence,
                            is_out,
                            edge_type) != 0) {
    return false;
  }

  low = first;
  high = metadata_.record_count;
  while (low < high) {
    const std::uint64_t middle = low + (high - low) / 2;
    const DeltaDiskRecord record =
        LoadDiskRecord(mapping_, records_offset_, middle);
    const int key_comparison = CompareDiskLogicalKey(record,
                                                      src,
                                                      dst,
                                                      base_sequence,
                                                      is_out,
                                                      edge_type);
    if (key_comparison < 0 ||
        (key_comparison == 0 &&
         record.commit_sequence <= read_sequence)) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  if (low == first) return false;
  const DeltaDiskRecord selected =
      LoadDiskRecord(mapping_, records_offset_, low - 1);
  if (CompareDiskLogicalKey(selected,
                            src,
                            dst,
                            base_sequence,
                            is_out,
                            edge_type) != 0 ||
      selected.commit_sequence > read_sequence) {
    return false;
  }
  const std::uint64_t begin = values_offset_ + selected.value_offset;
  const std::uint64_t end = begin + selected.value_length;
  if (end > mapping_size_) return false;
  value->assign(reinterpret_cast<const char*>(mapping_ + begin),
                selected.value_length);
  if (found_commit != nullptr) {
    *found_commit = selected.commit_sequence;
  }
  return true;
}

bool PropertyDeltaFile::ForEachRecord(
    const RecordVisitor& visitor) const {
  if (!visitor || mapping_ == nullptr) return false;
  for (std::uint64_t i = 0; i < metadata_.record_count; ++i) {
    const DeltaDiskRecord disk =
        LoadDiskRecord(mapping_, records_offset_, i);
    const std::uint64_t end = values_offset_ + disk.value_offset +
                              disk.value_length;
    if (end > mapping_size_) return false;
    if (!visitor(DiskRecordToRecord(disk, mapping_, values_offset_))) {
      return false;
    }
  }
  return true;
}

std::shared_ptr<MappedPropertyFile> MappedPropertyFile::Open(
    const std::string& path,
    std::uint64_t generation,
    std::string* error) {
  auto file = std::shared_ptr<MappedPropertyFile>(new MappedPropertyFile());
  file->generation_ = generation;
  file->fd_ = ::open(path.c_str(), O_RDONLY);
  if (file->fd_ < 0) {
    SetError(error, ErrnoMessage("open", path));
    return nullptr;
  }
  struct stat status {};
  if (::fstat(file->fd_, &status) != 0 || status.st_size <= 0) {
    SetError(error, ErrnoMessage("fstat", path));
    return nullptr;
  }
  file->mapping_size_ = static_cast<std::size_t>(status.st_size);
  void* mapping = ::mmap(nullptr,
                         file->mapping_size_,
                         PROT_READ,
                         MAP_PRIVATE,
                         file->fd_,
                         0);
  if (mapping == MAP_FAILED) {
    SetError(error, ErrnoMessage("mmap", path));
    return nullptr;
  }
  file->mapping_ = static_cast<const std::byte*>(mapping);
  ::madvise(const_cast<std::byte*>(file->mapping_),
            file->mapping_size_,
            MADV_RANDOM);
  return file;
}

MappedPropertyFile::~MappedPropertyFile() {
  if (mapping_ != nullptr) {
    ::munmap(const_cast<std::byte*>(mapping_), mapping_size_);
    mapping_ = nullptr;
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

}  // namespace lsmgraph
