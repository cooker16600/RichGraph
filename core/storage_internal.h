#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "core/types.h"

namespace lsmgraph {

class SSTableCache;

// Temporarily route legacy storage-path helpers to a specific GraphDb shard.
// The override is thread-local so concurrent databases do not share paths.
const std::string* GetCurrentDbPathOverride();

class ScopedDbPathOverride {
 public:
  explicit ScopedDbPathOverride(const std::string* db_path);
  ~ScopedDbPathOverride();

  ScopedDbPathOverride(const ScopedDbPathOverride&) = delete;
  ScopedDbPathOverride& operator=(const ScopedDbPathOverride&) = delete;

 private:
  const std::string* previous_;
};

const std::string& GetCurrentDbPath();

std::string eFileName(uint64_t current_time);
std::string pFileName(uint64_t current_time);
std::string pFileName_with_id(const std::string& property_file, int id);
VertexId_t get_dst(const char* data);
SequenceNumber_t get_seq(const char* data);
Marker_t get_marker(const char* data);

size_t GetFileSize_(const char* file_name);
int getLevelMaxSize(int level);

bool cacheTimeCompare(const SSTableCache* lhs, const SSTableCache* rhs);
bool cacheKeyCompare(const SSTableCache* lhs, const SSTableCache* rhs);

}  // namespace lsmgraph
