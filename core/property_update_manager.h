#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "core/property_delta.h"
#include "richgraph/options.h"

namespace lsmgraph {

struct BufferedPropertyUpdate {
  PropertyDeltaTarget target;
  PropertyDeltaRecord record;
  bool target_is_persistent{false};
};

// A scan copies the matching in-buffer values before it captures the delta
// views.  That ordering closes the PropertyBuffer -> delta publication race:
// an update is then present either in this snapshot or in the subsequently
// captured delta view.
struct BufferedPropertySnapshotEntry {
  VertexId_t dst{0};
  SequenceNumber_t base_sequence{0};
  PropertyCommitSequence commit_sequence{0};
  std::string value;
};

using PropertyUpdateFlushCallback = std::function<bool(
    const PropertyDeltaTarget& target,
    std::vector<PropertyDeltaRecord> records,
    bool target_is_persistent,
    std::string* error)>;

// Engine-owned buffering and flush coordination for property updates.
// Visibility protocol:
//  1. Submit publishes the newest value in pending_ before acknowledging it.
//  2. The worker creates and publishes the durable delta file.
//  3. Only after publication succeeds does it conditionally erase pending_.
// A reader therefore observes the update in at least one layer throughout the
// hand-off. Flush order is FIFO, so a pending value is always newer than the
// already-published delta chain for the same logical key.
class PropertyUpdateManager {
 public:
  PropertyUpdateManager(const PropertyUpdateOptions& options,
                        PropertyUpdateFlushCallback flush_callback,
                        PropertyCommitSequence initial_commit_sequence = 1);
  ~PropertyUpdateManager();

  PropertyUpdateManager(const PropertyUpdateManager&) = delete;
  PropertyUpdateManager& operator=(const PropertyUpdateManager&) = delete;

  [[nodiscard]] bool Submit(BufferedPropertyUpdate update,
                            PropertyCommitSequence* commit_sequence,
                            std::string* error);

  // Publishes one logical update containing one or more physical property
  // records. Every record receives the same commit sequence, and the batch is
  // rejected before publication when it cannot fit in one Property Buffer.
  // Keeping a logical update in one buffer also prevents a rotation boundary
  // from splitting it across independently flushed buffers.
  [[nodiscard]] bool SubmitBatch(
      std::vector<BufferedPropertyUpdate> updates,
      PropertyCommitSequence* commit_sequence,
      std::string* error);

  [[nodiscard]] bool LookupLatest(PropertyObjectKind kind,
                                  std::uint32_t shard_id,
                                  std::uint32_t property_id,
                                  VertexId_t src,
                                  VertexId_t dst,
                                  bool is_out,
                                  std::uint8_t edge_type,
                                  std::string* value,
                                  PropertyCommitSequence* commit_sequence =
                                      nullptr) const;

  [[nodiscard]] bool LookupExact(PropertyObjectKind kind,
                                 std::uint32_t shard_id,
                                 std::uint32_t property_id,
                                 VertexId_t src,
                                 VertexId_t dst,
                                 SequenceNumber_t base_sequence,
                                 bool is_out,
                                 std::uint8_t edge_type,
                                 std::string* value,
                                 PropertyCommitSequence* commit_sequence =
                                     nullptr) const;

  [[nodiscard]] std::vector<BufferedPropertySnapshotEntry>
  SnapshotForSource(PropertyObjectKind kind,
                    std::uint32_t shard_id,
                    std::uint32_t property_id,
                    VertexId_t src,
                    bool is_out,
                    std::uint8_t edge_type) const;

  [[nodiscard]] bool FlushAndWait(std::string* error);
  [[nodiscard]] PropertyUpdateManagerStats Stats() const;
  [[nodiscard]] std::string LastError() const;

 private:
  struct PendingKey {
    PropertyObjectKind kind{PropertyObjectKind::kEdge};
    std::uint32_t shard_id{0};
    std::uint32_t property_id{0};
    VertexId_t src{0};
    VertexId_t dst{0};
    bool is_out{true};
    std::uint8_t edge_type{0};

    bool operator==(const PendingKey& other) const;
  };

  struct PendingKeyHash {
    std::size_t operator()(const PendingKey& key) const;
  };

  struct PendingValue {
    SequenceNumber_t base_sequence{0};
    PropertyCommitSequence commit_sequence{0};
    std::string value;
  };

  struct PendingBucket {
    mutable std::shared_mutex mutex;
    std::unordered_map<PendingKey, PendingValue, PendingKeyHash> values;
  };

  enum class BufferState { kFree, kActive, kQueued, kFlushing, kFailed };
  struct Buffer {
    BufferState state{BufferState::kFree};
    std::vector<BufferedPropertyUpdate> records;
    std::size_t bytes{0};
  };

  static constexpr std::size_t kPendingBucketCount = 64;
  static constexpr std::size_t kNoBuffer = static_cast<std::size_t>(-1);

  PendingKey MakePendingKey(const BufferedPropertyUpdate& update) const;
  PendingKey MakePendingKey(PropertyObjectKind kind,
                            std::uint32_t shard_id,
                            std::uint32_t property_id,
                            VertexId_t src,
                            VertexId_t dst,
                            bool is_out,
                            std::uint8_t edge_type) const;
  std::size_t PendingBucketIndex(const PendingKey& key) const;
  void PublishPending(const BufferedPropertyUpdate& update);
  void RetirePending(const std::vector<BufferedPropertyUpdate>& records);

  std::size_t EstimateBytes(const BufferedPropertyUpdate& update) const;
  bool EnsureActiveBuffer(std::unique_lock<std::mutex>* lock,
                          std::string* error);
  void QueueActiveBuffer();
  bool FlushBuffer(std::size_t buffer_index, std::string* error);
  void WorkerMain();
  void StopWorker();
  void SetFatalError(std::string error);

  PropertyUpdateOptions options_;
  PropertyUpdateFlushCallback flush_callback_;
  std::atomic<PropertyCommitSequence> next_commit_sequence_{1};

  mutable std::array<PendingBucket, kPendingBucketCount> pending_;

  mutable std::mutex mutex_;
  std::condition_variable worker_cv_;
  std::condition_variable free_cv_;
  std::condition_variable idle_cv_;
  std::vector<Buffer> buffers_;
  std::deque<std::size_t> queued_buffers_;
  std::size_t active_buffer_{kNoBuffer};
  std::uint64_t running_flushes_{0};
  bool stop_{false};
  std::string fatal_error_;
  std::thread worker_;

  std::atomic<std::uint64_t> submitted_records_{0};
  std::atomic<std::uint64_t> submitted_bytes_{0};
  std::atomic<std::uint64_t> buffers_flushed_{0};
  std::atomic<std::uint64_t> delta_batches_published_{0};
  std::atomic<std::uint64_t> flush_failures_{0};
  std::atomic<std::uint64_t> blocked_submissions_{0};
};

}  // namespace lsmgraph
