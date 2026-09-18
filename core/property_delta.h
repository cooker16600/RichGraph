#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "core/types.h"
#include "richgraph/options.h"

namespace lsmgraph {

enum class PropertyObjectKind : std::uint8_t {
  kNode = 1,
  kEdge = 2,
};

// Identifies one physical property column. base_generation is deliberately
// distinct from base_file_id so a future file-id reuse cannot attach stale
// deltas to a different SST generation.
struct PropertyDeltaTarget {
  PropertyObjectKind kind{PropertyObjectKind::kEdge};
  std::uint32_t shard_id{0};
  FileId_t base_file_id{INVALID_File_ID};
  std::uint64_t base_generation{0};
  std::uint32_t property_id{0};

  bool operator==(const PropertyDeltaTarget& other) const;
};

struct PropertyDeltaTargetHash {
  std::size_t operator()(const PropertyDeltaTarget& target) const;
};

// A property update refers to an existing topology record. base_sequence is
// the immutable topology record sequence; commit_sequence orders property
// versions independently of topology insertion order.
struct PropertyDeltaRecord {
  VertexId_t src{0};
  VertexId_t dst{0};
  SequenceNumber_t base_sequence{0};
  PropertyCommitSequence commit_sequence{0};
  bool is_out{true};
  std::uint8_t edge_type{0};
  std::string value;
};

struct PropertyDeltaFileMetadata {
  PropertyDeltaTarget target;
  std::uint64_t file_generation{0};
  std::uint64_t record_count{0};
  std::uint64_t file_bytes{0};
  PropertyCommitSequence min_commit_sequence{0};
  PropertyCommitSequence max_commit_sequence{0};
  std::uint64_t payload_checksum{0};
};

// Immutable, mmap-backed delta file. The class owns both fd and mapping and
// unlinks an obsolete file only after the last read view releases it.
class PropertyDeltaFile {
 public:
  using RecordVisitor = std::function<bool(const PropertyDeltaRecord&)>;

  static std::shared_ptr<PropertyDeltaFile> Create(
      const std::string& path,
      const PropertyDeltaTarget& target,
      std::uint64_t file_generation,
      std::vector<PropertyDeltaRecord> records,
      DeltaDurability durability,
      std::string* error);

  static std::shared_ptr<PropertyDeltaFile> Open(const std::string& path,
                                                 std::string* error);

  ~PropertyDeltaFile();

  PropertyDeltaFile(const PropertyDeltaFile&) = delete;
  PropertyDeltaFile& operator=(const PropertyDeltaFile&) = delete;

  // Returns the newest version of key whose commit sequence is <=
  // read_sequence. When found_commit is non-null it receives that version.
  [[nodiscard]] bool Lookup(
      VertexId_t src,
      VertexId_t dst,
      SequenceNumber_t base_sequence,
      bool is_out,
      std::uint8_t edge_type,
      PropertyCommitSequence read_sequence,
      std::string* value,
      PropertyCommitSequence* found_commit = nullptr) const;

  [[nodiscard]] bool ForEachRecord(const RecordVisitor& visitor) const;

  const PropertyDeltaFileMetadata& metadata() const { return metadata_; }
  const std::string& path() const { return path_; }

  void MarkObsolete() { obsolete_.store(true, std::memory_order_release); }

 private:
  PropertyDeltaFile() = default;

  [[nodiscard]] bool OpenAndValidate(const std::string& path,
                                     std::string* error);

  std::string path_;
  int fd_{-1};
  const std::byte* mapping_{nullptr};
  std::size_t mapping_size_{0};
  std::uint64_t records_offset_{0};
  std::uint64_t values_offset_{0};
  PropertyDeltaFileMetadata metadata_;
  std::atomic<bool> obsolete_{false};
};

// Owns one immutable mapping of a merged SST property column. Replacing the
// pathname does not invalidate an existing mapping; old snapshots therefore
// remain safe until their shared_ptr is released.
class MappedPropertyFile {
 public:
  static std::shared_ptr<MappedPropertyFile> Open(const std::string& path,
                                                  std::uint64_t generation,
                                                  std::string* error);

  ~MappedPropertyFile();

  MappedPropertyFile(const MappedPropertyFile&) = delete;
  MappedPropertyFile& operator=(const MappedPropertyFile&) = delete;

  const std::byte* data() const { return mapping_; }
  std::size_t size() const { return mapping_size_; }
  std::uint64_t generation() const { return generation_; }

 private:
  MappedPropertyFile() = default;

  int fd_{-1};
  const std::byte* mapping_{nullptr};
  std::size_t mapping_size_{0};
  std::uint64_t generation_{0};
};

}  // namespace lsmgraph
