#include "core/storage_internal.h"

#include <cstring>
#include <sys/stat.h>

#include "core/SSTable.h"
#include "core/config.h"
#include "core/flags.h"

namespace lsmgraph {
namespace {

thread_local const std::string* tls_db_path_override = nullptr;

uint32_t ReadUint32(const char* data) {
  uint32_t value = 0;
  std::memcpy(&value, data, sizeof(value));
  return value;
}

}  // namespace

const std::string* GetCurrentDbPathOverride() {
  return tls_db_path_override;
}

ScopedDbPathOverride::ScopedDbPathOverride(const std::string* db_path)
    : previous_(tls_db_path_override) {
  tls_db_path_override = db_path;
}

ScopedDbPathOverride::~ScopedDbPathOverride() {
  tls_db_path_override = previous_;
}

const std::string& GetCurrentDbPath() {
  if (tls_db_path_override != nullptr) {
    return *tls_db_path_override;
  }
  return FLAGS_db_path;
}

std::string eFileName(uint64_t current_time) {
  return GetCurrentDbPath() + "/" + std::to_string(current_time) + ".sst";
}

std::string pFileName(uint64_t current_time) {
  return GetCurrentDbPath() + "/" + std::to_string(current_time) + ".sst_p";
}

std::string pFileName_with_id(const std::string& property_file, int id) {
  return property_file + "_" + std::to_string(id);
}

VertexId_t get_dst(const char* data) {
  const uint32_t low = ReadUint32(data + 4);
  const uint32_t high_part = ReadUint32(data + 12);
  const uint16_t high = high_part & UINT16_MAX;
  return low | (static_cast<uint64_t>(high) << 32);
}

SequenceNumber_t get_seq(const char* data) {
  const uint32_t low = ReadUint32(data + 8);
  const uint32_t high_part = ReadUint32(data + 12);
  const uint16_t high = (high_part >> 16) & UINT16_MAX;
  return low | (static_cast<uint64_t>(high) << 32);
}

Marker_t get_marker(const char* data) {
  return (ReadUint32(data) >> 31) & 0x1;
}

size_t GetFileSize_(const char* file_name) {
  if (file_name == nullptr) {
    return 0;
  }
  struct stat file_status {};
  if (stat(file_name, &file_status) != 0) {
    return 0;
  }
  return static_cast<size_t>(file_status.st_size);
}

int getLevelMaxSize(int level) {
  int max_files = 4;
  for (int current_level = 0; current_level < level; ++current_level) {
    max_files *= LEVEL_FILE_RATIO;
  }
  return max_files;
}

bool cacheTimeCompare(const SSTableCache* lhs, const SSTableCache* rhs) {
  return lhs->header.timeStamp > rhs->header.timeStamp;
}

bool cacheKeyCompare(const SSTableCache* lhs, const SSTableCache* rhs) {
  return lhs->header.minKey < rhs->header.minKey;
}

}  // namespace lsmgraph
