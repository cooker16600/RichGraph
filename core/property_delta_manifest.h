#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "core/property_delta.h"

namespace lsmgraph {

struct PropertyDeltaManifestEntry {
  std::string file_name;
  PropertyDeltaFileMetadata metadata;
  // False means the delta was attached to a mutable MemTable whose topology
  // was not durable yet. Recovery discards such an entry unless the
  bool target_is_persistent{false};
};

// Append-only commit log for published property deltas. The manifest never
// stores absolute paths, which keeps a database directory relocatable.
class PropertyDeltaManifest {
 public:
  static std::unique_ptr<PropertyDeltaManifest> Open(
      const std::string& directory,
      DeltaDurability durability,
      std::string* error);

  ~PropertyDeltaManifest();

  PropertyDeltaManifest(const PropertyDeltaManifest&) = delete;
  PropertyDeltaManifest& operator=(const PropertyDeltaManifest&) = delete;

  [[nodiscard]] bool AppendAdd(const PropertyDeltaManifestEntry& entry,
                               std::string* error);

  [[nodiscard]] bool AppendRemove(const std::vector<std::string>& file_names,
                                  std::string* error);

  [[nodiscard]] std::vector<PropertyDeltaManifestEntry> ActiveEntries() const;

  const std::string& directory() const { return directory_; }
  const std::string& path() const { return path_; }

 private:
  PropertyDeltaManifest() = default;

  [[nodiscard]] bool OpenAndRecover(const std::string& directory,
                                    DeltaDurability durability,
                                    std::string* error);
  [[nodiscard]] bool AppendRecord(std::uint8_t type,
                                  const std::vector<std::byte>& payload,
                                  std::string* error);

  std::string directory_;
  std::string path_;
  int fd_{-1};
  DeltaDurability durability_{DeltaDurability::kProcessCrashSafe};
  std::uint64_t next_edit_sequence_{1};

  mutable std::mutex mutex_;
  std::vector<PropertyDeltaManifestEntry> active_entries_;
};

}  // namespace lsmgraph
