#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "core/property_delta.h"
#include "core/property_delta_manifest.h"

namespace lsmgraph {

struct PropertyDeltaLookupCounters {
  std::atomic<std::uint64_t> lookups{0};
  std::atomic<std::uint64_t> files_probed{0};
  std::atomic<std::uint64_t> hits{0};
  std::atomic<std::uint64_t> base_fallbacks{0};
};

struct PropertyDeltaReadView {
  std::shared_ptr<const MappedPropertyFile> merged_base;
  // Files are ordered by file generation, oldest to newest.
  std::vector<std::shared_ptr<PropertyDeltaFile>> deltas;
  std::shared_ptr<PropertyDeltaLookupCounters> counters;
  std::uint64_t generation{0};

  [[nodiscard]] bool Lookup(
      VertexId_t src,
      VertexId_t dst,
      SequenceNumber_t base_sequence,
      bool is_out,
      std::uint8_t edge_type,
      PropertyCommitSequence read_sequence,
      std::string* value,
      PropertyCommitSequence* found_commit = nullptr) const;

  const std::byte* BaseDataOr(const void* original) const {
    return merged_base == nullptr
               ? static_cast<const std::byte*>(original)
               : merged_base->data();
  }
};

struct PropertyDeltaStoreStats {
  std::uint64_t files_created{0};
  std::uint64_t files_recovered{0};
  std::uint64_t records_written{0};
  std::uint64_t bytes_written{0};
  std::uint64_t merges_started{0};
  std::uint64_t merges_completed{0};
  std::uint64_t merge_failures{0};
  std::uint64_t chain_count{0};
  std::uint64_t visible_files{0};
  std::uint64_t max_chain_length{0};
  std::uint64_t lookups{0};
  std::uint64_t files_probed{0};
  std::uint64_t hits{0};
  std::uint64_t base_fallbacks{0};
};

// The callback materializes a new merged property column. It receives the
// current replacement base (or null for the original SST mapping), the exact
// chain prefix to consume, and the output generation. It must return a mapping
// of the fully synchronized replacement file.
using PropertyDeltaMergeCallback = std::function<bool(
    const PropertyDeltaTarget& target,
    const std::shared_ptr<const MappedPropertyFile>& current_base,
    const std::vector<std::shared_ptr<PropertyDeltaFile>>& deltas,
    std::uint64_t output_generation,
    std::shared_ptr<MappedPropertyFile>* merged_base,
    std::string* error)>;

class PropertyDeltaStore {
 public:
  static std::unique_ptr<PropertyDeltaStore> Open(
      const std::string& directory,
      std::uint32_t merge_threshold,
      DeltaDurability durability,
      PropertyDeltaMergeCallback merge_callback,
      std::string* error);

  ~PropertyDeltaStore();

  PropertyDeltaStore(const PropertyDeltaStore&) = delete;
  PropertyDeltaStore& operator=(const PropertyDeltaStore&) = delete;

  // Creates, commits, then publishes one immutable delta file.
  [[nodiscard]] bool Attach(const PropertyDeltaTarget& target,
                            std::vector<PropertyDeltaRecord> records,
                            bool target_is_persistent,
                            std::string* error);

  [[nodiscard]] std::shared_ptr<const PropertyDeltaReadView> ReadView(
      const PropertyDeltaTarget& target) const;

  // A MemTable and the SST produced from it retain the same file ID. This
  // transition makes a previously non-mergeable chain eligible for merge.
  [[nodiscard]] bool MarkTargetPersistent(
      const PropertyDeltaTarget& target,
      std::string* error = nullptr);

  // Waits for already scheduled merge work. It does not force a below-
  // threshold chain to merge.
  [[nodiscard]] bool WaitForIdle(std::string* error);

  // Synchronously merges every visible delta for the selected base files.
  // This is the compaction preparation primitive.
  [[nodiscard]] bool MergeAllForBaseFiles(
      const std::vector<FileId_t>& base_file_ids,
      std::string* error);

  // Drains every persistent chain, including chains below the normal
  // threshold.  The compaction coordinator uses this while new attachments
  // are externally blocked so a compaction never consumes a stale base file.
  [[nodiscard]] bool MergeAllPersistent(std::string* error);

  [[nodiscard]] PropertyDeltaStoreStats Stats() const;
  [[nodiscard]] PropertyCommitSequence MaxCommitSequence() const {
    return maximum_commit_sequence_.load(std::memory_order_acquire);
  }
  [[nodiscard]] std::string LastBackgroundError() const;

 private:
  struct Chain {
    mutable std::mutex mutex;
    std::shared_ptr<const PropertyDeltaReadView> read_view;
    bool target_is_persistent{false};
    bool merge_scheduled_or_running{false};
  };

  struct RegistryBucket {
    mutable std::shared_mutex mutex;
    std::unordered_map<PropertyDeltaTarget,
                       std::shared_ptr<Chain>,
                       PropertyDeltaTargetHash>
        chains;
  };

  struct MergeJob {
    PropertyDeltaTarget target;
    std::shared_ptr<Chain> chain;
  };

  static constexpr std::size_t kRegistryBucketCount = 64;

  PropertyDeltaStore() = default;

  [[nodiscard]] bool OpenAndRecover(
      const std::string& directory,
      std::uint32_t merge_threshold,
      DeltaDurability durability,
      PropertyDeltaMergeCallback merge_callback,
      std::string* error);

  std::size_t BucketIndex(const PropertyDeltaTarget& target) const;
  std::shared_ptr<Chain> GetOrCreateChain(
      const PropertyDeltaTarget& target,
      bool target_is_persistent);
  std::shared_ptr<Chain> FindChain(
      const PropertyDeltaTarget& target) const;

  void ScheduleMerge(const PropertyDeltaTarget& target,
                     const std::shared_ptr<Chain>& chain);
  void WorkerMain();
  [[nodiscard]] bool MergeChain(const PropertyDeltaTarget& target,
                                const std::shared_ptr<Chain>& chain,
                                bool merge_all,
                                std::string* error);
  void StopWorker();
  void RecordBackgroundError(std::string error);
  void RemoveOrphanFiles();

  std::string directory_;
  std::uint32_t merge_threshold_{0};
  DeltaDurability durability_{DeltaDurability::kProcessCrashSafe};
  PropertyDeltaMergeCallback merge_callback_;
  std::unique_ptr<PropertyDeltaManifest> manifest_;
  std::array<RegistryBucket, kRegistryBucketCount> registry_;
  std::shared_ptr<PropertyDeltaLookupCounters> lookup_counters_ =
      std::make_shared<PropertyDeltaLookupCounters>();
  std::atomic<std::uint64_t> next_file_generation_{1};
  std::atomic<PropertyCommitSequence> maximum_commit_sequence_{0};

  mutable std::mutex persistent_targets_mutex_;
  std::unordered_set<FileId_t> persistent_base_files_;

  mutable std::mutex worker_mutex_;
  std::condition_variable worker_cv_;
  std::condition_variable idle_cv_;
  std::deque<MergeJob> jobs_;
  std::thread worker_;
  bool stop_{false};
  std::uint64_t running_jobs_{0};
  std::string background_error_;

  std::atomic<std::uint64_t> files_created_{0};
  std::atomic<std::uint64_t> files_recovered_{0};
  std::atomic<std::uint64_t> records_written_{0};
  std::atomic<std::uint64_t> bytes_written_{0};
  std::atomic<std::uint64_t> merges_started_{0};
  std::atomic<std::uint64_t> merges_completed_{0};
  std::atomic<std::uint64_t> merge_failures_{0};
};

}  // namespace lsmgraph
