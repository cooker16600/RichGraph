#include "core/property_delta_manifest.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>

namespace lsmgraph {
namespace {

constexpr std::array<char, 8> kManifestMagic{
    {'R', 'G', 'M', 'A', 'N', 'I', 'F', '3'}};
constexpr std::uint32_t kManifestVersion = 3;
constexpr std::uint8_t kAddRecord = 1;
constexpr std::uint8_t kRemoveRecord = 2;

#pragma pack(push, 1)
struct ManifestDiskHeader {
  char magic[8];
  std::uint32_t version;
  std::uint8_t type;
  std::uint8_t reserved[3];
  std::uint32_t payload_bytes;
  std::uint64_t edit_sequence;
  std::uint64_t checksum;
};

struct ManifestAddPayload {
  std::uint8_t object_kind;
  std::uint8_t target_is_persistent;
  std::uint8_t reserved0[2];
  std::uint32_t shard_id;
  std::uint32_t property_id;
  std::uint32_t base_file_id;
  std::uint64_t base_generation;
  std::uint64_t file_generation;
  std::uint64_t record_count;
  std::uint64_t file_bytes;
  std::uint64_t min_commit_sequence;
  std::uint64_t max_commit_sequence;
  std::uint64_t payload_checksum;
  std::uint32_t file_name_bytes;
};
#pragma pack(pop)

static_assert(sizeof(ManifestDiskHeader) == 36,
              "unexpected delta-manifest record header layout");
static_assert(sizeof(ManifestAddPayload) == 76,
              "unexpected delta-manifest add payload layout");

std::string ErrnoMessage(std::string_view operation,
                         const std::string& path) {
  return std::string(operation) + " " + path + ": " + std::strerror(errno);
}

void SetError(std::string* error, std::string message) {
  if (error != nullptr) *error = std::move(message);
}

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

std::uint64_t RecordChecksum(ManifestDiskHeader header,
                             const std::vector<std::byte>& payload) {
  constexpr std::uint64_t kOffset = 14695981039346656037ULL;
  header.checksum = 0;
  std::uint64_t hash = ExtendChecksum(kOffset, &header, sizeof(header));
  if (!payload.empty()) {
    hash = ExtendChecksum(hash, payload.data(), payload.size());
  }
  return hash;
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
      if (errno == EINTR) continue;
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

bool ReadAllAt(int fd,
               void* data,
               std::size_t size,
               off_t offset,
               const std::string& path,
               std::string* error) {
  auto* cursor = static_cast<std::byte*>(data);
  std::size_t remaining = size;
  while (remaining != 0) {
    const ssize_t count = ::pread(fd, cursor, remaining, offset);
    if (count < 0) {
      if (errno == EINTR) continue;
      SetError(error, ErrnoMessage("pread", path));
      return false;
    }
    if (count == 0) return false;
    cursor += count;
    offset += count;
    remaining -= static_cast<std::size_t>(count);
  }
  return true;
}

template <typename T>
void AppendPod(std::vector<std::byte>* payload, const T& value) {
  const auto* begin = reinterpret_cast<const std::byte*>(&value);
  payload->insert(payload->end(), begin, begin + sizeof(T));
}

template <typename T>
bool ConsumePod(const std::vector<std::byte>& payload,
                std::size_t* offset,
                T* value) {
  if (offset == nullptr || value == nullptr ||
      *offset > payload.size() || sizeof(T) > payload.size() - *offset) {
    return false;
  }
  std::memcpy(value, payload.data() + *offset, sizeof(T));
  *offset += sizeof(T);
  return true;
}

bool ValidFileName(const std::string& file_name) {
  if (file_name.empty()) return false;
  const std::filesystem::path path(file_name);
  return !path.is_absolute() && path.filename() == path &&
         file_name != "." && file_name != "..";
}

bool ValidObjectKind(std::uint8_t value) {
  return value == static_cast<std::uint8_t>(PropertyObjectKind::kNode) ||
         value == static_cast<std::uint8_t>(PropertyObjectKind::kEdge);
}

std::vector<std::byte> EncodeAdd(const PropertyDeltaManifestEntry& entry) {
  ManifestAddPayload fixed{};
  fixed.object_kind = static_cast<std::uint8_t>(entry.metadata.target.kind);
  fixed.target_is_persistent = entry.target_is_persistent ? 1 : 0;
  fixed.shard_id = entry.metadata.target.shard_id;
  fixed.property_id = entry.metadata.target.property_id;
  fixed.base_file_id = entry.metadata.target.base_file_id;
  fixed.base_generation = entry.metadata.target.base_generation;
  fixed.file_generation = entry.metadata.file_generation;
  fixed.record_count = entry.metadata.record_count;
  fixed.file_bytes = entry.metadata.file_bytes;
  fixed.min_commit_sequence = entry.metadata.min_commit_sequence;
  fixed.max_commit_sequence = entry.metadata.max_commit_sequence;
  fixed.payload_checksum = entry.metadata.payload_checksum;
  fixed.file_name_bytes = static_cast<std::uint32_t>(entry.file_name.size());
  std::vector<std::byte> payload;
  payload.reserve(sizeof(fixed) + entry.file_name.size());
  AppendPod(&payload, fixed);
  const auto* name =
      reinterpret_cast<const std::byte*>(entry.file_name.data());
  payload.insert(payload.end(), name, name + entry.file_name.size());
  return payload;
}

bool DecodeAdd(const std::vector<std::byte>& payload,
               PropertyDeltaManifestEntry* entry) {
  if (entry == nullptr || payload.size() < sizeof(ManifestAddPayload)) {
    return false;
  }
  ManifestAddPayload fixed{};
  std::memcpy(&fixed, payload.data(), sizeof(fixed));
  if (!ValidObjectKind(fixed.object_kind) ||
      fixed.target_is_persistent > 1 ||
      fixed.file_name_bytes != payload.size() - sizeof(fixed)) {
    return false;
  }
  entry->file_name.assign(
      reinterpret_cast<const char*>(payload.data() + sizeof(fixed)),
      fixed.file_name_bytes);
  if (!ValidFileName(entry->file_name)) return false;
  entry->metadata.target.kind =
      static_cast<PropertyObjectKind>(fixed.object_kind);
  entry->metadata.target.shard_id = fixed.shard_id;
  entry->metadata.target.property_id = fixed.property_id;
  entry->metadata.target.base_file_id = fixed.base_file_id;
  entry->metadata.target.base_generation = fixed.base_generation;
  entry->metadata.file_generation = fixed.file_generation;
  entry->metadata.record_count = fixed.record_count;
  entry->metadata.file_bytes = fixed.file_bytes;
  entry->metadata.min_commit_sequence = fixed.min_commit_sequence;
  entry->metadata.max_commit_sequence = fixed.max_commit_sequence;
  entry->metadata.payload_checksum = fixed.payload_checksum;
  entry->target_is_persistent = fixed.target_is_persistent != 0;
  return true;
}

std::vector<std::byte> EncodeRemove(
    const std::vector<std::string>& file_names) {
  std::vector<std::byte> payload;
  const std::uint32_t count = static_cast<std::uint32_t>(file_names.size());
  AppendPod(&payload, count);
  for (const auto& file_name : file_names) {
    const std::uint32_t length = static_cast<std::uint32_t>(file_name.size());
    AppendPod(&payload, length);
    const auto* bytes =
        reinterpret_cast<const std::byte*>(file_name.data());
    payload.insert(payload.end(), bytes, bytes + file_name.size());
  }
  return payload;
}

bool DecodeRemove(const std::vector<std::byte>& payload,
                  std::vector<std::string>* file_names) {
  if (file_names == nullptr) return false;
  std::size_t offset = 0;
  std::uint32_t count = 0;
  if (!ConsumePod(payload, &offset, &count)) return false;
  file_names->clear();
  file_names->reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint32_t length = 0;
    if (!ConsumePod(payload, &offset, &length) ||
        offset > payload.size() || length > payload.size() - offset) {
      return false;
    }
    std::string name(reinterpret_cast<const char*>(payload.data() + offset),
                     length);
    offset += length;
    if (!ValidFileName(name)) return false;
    file_names->push_back(std::move(name));
  }
  return offset == payload.size();
}

void ApplyAdd(std::vector<PropertyDeltaManifestEntry>* active,
              PropertyDeltaManifestEntry entry) {
  auto it = std::find_if(active->begin(), active->end(), [&](const auto& item) {
    return item.file_name == entry.file_name;
  });
  if (it == active->end()) {
    active->push_back(std::move(entry));
  } else {
    *it = std::move(entry);
  }
}

void ApplyRemove(std::vector<PropertyDeltaManifestEntry>* active,
                 const std::vector<std::string>& file_names) {
  active->erase(
      std::remove_if(active->begin(), active->end(), [&](const auto& entry) {
        return std::find(file_names.begin(), file_names.end(), entry.file_name) !=
               file_names.end();
      }),
      active->end());
}

}  // namespace

std::unique_ptr<PropertyDeltaManifest> PropertyDeltaManifest::Open(
    const std::string& directory,
    DeltaDurability durability,
    std::string* error) {
  auto manifest = std::unique_ptr<PropertyDeltaManifest>(
      new PropertyDeltaManifest());
  if (!manifest->OpenAndRecover(directory, durability, error)) {
    return nullptr;
  }
  return manifest;
}

bool PropertyDeltaManifest::OpenAndRecover(const std::string& directory,
                                           DeltaDurability durability,
                                           std::string* error) {
  directory_ = directory;
  durability_ = durability;
  std::error_code filesystem_error;
  std::filesystem::create_directories(directory_, filesystem_error);
  if (filesystem_error) {
    SetError(error,
             "create delta directory " + directory_ + ": " +
                 filesystem_error.message());
    return false;
  }
  path_ = (std::filesystem::path(directory_) / "MANIFEST").string();
  fd_ = ::open(path_.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
  if (fd_ < 0) {
    SetError(error, ErrnoMessage("open", path_));
    return false;
  }
  struct stat status {};
  if (::fstat(fd_, &status) != 0 || status.st_size < 0) {
    SetError(error, ErrnoMessage("fstat", path_));
    return false;
  }

  const off_t file_size = status.st_size;
  off_t offset = 0;
  off_t valid_end = 0;
  std::uint64_t last_sequence = 0;
  while (offset < file_size) {
    if (file_size - offset < static_cast<off_t>(sizeof(ManifestDiskHeader))) {
      break;  // Torn final header.
    }
    ManifestDiskHeader header{};
    if (!ReadAllAt(fd_, &header, sizeof(header), offset, path_, error)) {
      break;
    }
    if (std::memcmp(header.magic, kManifestMagic.data(),
                    kManifestMagic.size()) != 0 ||
        header.version != kManifestVersion ||
        (header.type != kAddRecord && header.type != kRemoveRecord) ||
        header.payload_bytes > (1U << 30U)) {
      SetError(error, "invalid delta manifest header at offset " +
                          std::to_string(offset));
      return false;
    }
    const std::uint64_t record_size = sizeof(header) + header.payload_bytes;
    if (record_size > static_cast<std::uint64_t>(file_size - offset)) {
      break;  // Torn final payload.
    }
    std::vector<std::byte> payload(header.payload_bytes);
    if (!payload.empty() &&
        !ReadAllAt(fd_,
                   payload.data(),
                   payload.size(),
                   offset + sizeof(header),
                   path_,
                   error)) {
      break;
    }
    if (RecordChecksum(header, payload) != header.checksum) {
      if (offset + static_cast<off_t>(record_size) == file_size) {
        break;  // A torn final record can have a complete length field.
      }
      SetError(error, "delta manifest checksum mismatch at offset " +
                          std::to_string(offset));
      return false;
    }
    if (header.edit_sequence <= last_sequence) {
      SetError(error, "delta manifest edit sequence is not increasing");
      return false;
    }
    if (header.type == kAddRecord) {
      PropertyDeltaManifestEntry entry;
      if (!DecodeAdd(payload, &entry)) {
        SetError(error, "invalid delta manifest add payload");
        return false;
      }
      ApplyAdd(&active_entries_, std::move(entry));
    } else {
      std::vector<std::string> names;
      if (!DecodeRemove(payload, &names)) {
        SetError(error, "invalid delta manifest remove payload");
        return false;
      }
      ApplyRemove(&active_entries_, names);
    }
    last_sequence = header.edit_sequence;
    offset += static_cast<off_t>(record_size);
    valid_end = offset;
  }

  if (valid_end != file_size) {
    if (::ftruncate(fd_, valid_end) != 0) {
      SetError(error, ErrnoMessage("truncate torn manifest", path_));
      return false;
    }
    if (durability_ == DeltaDurability::kProcessCrashSafe &&
        ::fdatasync(fd_) != 0) {
      SetError(error, ErrnoMessage("fdatasync repaired manifest", path_));
      return false;
    }
  }
  next_edit_sequence_ = last_sequence + 1;
  return true;
}

PropertyDeltaManifest::~PropertyDeltaManifest() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

bool PropertyDeltaManifest::AppendRecord(
    std::uint8_t type,
    const std::vector<std::byte>& payload,
    std::string* error) {
  if (payload.size() > std::numeric_limits<std::uint32_t>::max()) {
    SetError(error, "delta manifest payload exceeds uint32 length");
    return false;
  }
  ManifestDiskHeader header{};
  std::memcpy(header.magic, kManifestMagic.data(), kManifestMagic.size());
  header.version = kManifestVersion;
  header.type = type;
  header.payload_bytes = static_cast<std::uint32_t>(payload.size());
  header.edit_sequence = next_edit_sequence_;
  header.checksum = RecordChecksum(header, payload);

  std::vector<std::byte> record;
  record.reserve(sizeof(header) + payload.size());
  AppendPod(&record, header);
  record.insert(record.end(), payload.begin(), payload.end());
  if (!WriteAll(fd_, record.data(), record.size(), path_, error)) {
    return false;
  }
  if (durability_ == DeltaDurability::kProcessCrashSafe &&
      ::fdatasync(fd_) != 0) {
    SetError(error, ErrnoMessage("fdatasync", path_));
    return false;
  }
  ++next_edit_sequence_;
  return true;
}

bool PropertyDeltaManifest::AppendAdd(
    const PropertyDeltaManifestEntry& entry,
    std::string* error) {
  if (!ValidFileName(entry.file_name)) {
    SetError(error, "delta manifest requires a basename");
    return false;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  const std::vector<std::byte> payload = EncodeAdd(entry);
  if (!AppendRecord(kAddRecord, payload, error)) return false;
  ApplyAdd(&active_entries_, entry);
  return true;
}

bool PropertyDeltaManifest::AppendRemove(
    const std::vector<std::string>& file_names,
    std::string* error) {
  if (file_names.empty()) return true;
  if (file_names.size() > std::numeric_limits<std::uint32_t>::max() ||
      std::any_of(file_names.begin(), file_names.end(), [](const auto& name) {
        return !ValidFileName(name) ||
               name.size() > std::numeric_limits<std::uint32_t>::max();
      })) {
    SetError(error, "invalid delta manifest remove filename");
    return false;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  const std::vector<std::byte> payload = EncodeRemove(file_names);
  if (!AppendRecord(kRemoveRecord, payload, error)) return false;
  ApplyRemove(&active_entries_, file_names);
  return true;
}

std::vector<PropertyDeltaManifestEntry>
PropertyDeltaManifest::ActiveEntries() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_entries_;
}

}  // namespace lsmgraph
