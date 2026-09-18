#include "core/property_update_manager.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace lsmgraph {
namespace {

void SetError(std::string* error, const std::string& message) {
  if (error != nullptr) *error = message;
}

template <typename T>
void HashCombine(std::size_t* seed, const T& value) {
  *seed ^= std::hash<T>{}(value) + 0x9e3779b97f4a7c15ULL +
           (*seed << 6U) + (*seed >> 2U);
}

}  // namespace

bool PropertyUpdateManager::PendingKey::operator==(
    const PendingKey& other) const {
  return kind == other.kind && shard_id == other.shard_id &&
         property_id == other.property_id && src == other.src &&
         dst == other.dst && is_out == other.is_out &&
         edge_type == other.edge_type;
}

std::size_t PropertyUpdateManager::PendingKeyHash::operator()(
    const PendingKey& key) const {
  std::size_t hash = 0;
  HashCombine(&hash, static_cast<std::uint8_t>(key.kind));
  HashCombine(&hash, key.shard_id);
  HashCombine(&hash, key.property_id);
  HashCombine(&hash, key.src);
  HashCombine(&hash, key.dst);
  HashCombine(&hash, key.is_out);
  HashCombine(&hash, key.edge_type);
  return hash;
}

PropertyUpdateManager::PropertyUpdateManager(
    const PropertyUpdateOptions& options,
    PropertyUpdateFlushCallback flush_callback,
    PropertyCommitSequence initial_commit_sequence)
    : options_(options),
      flush_callback_(std::move(flush_callback)),
      next_commit_sequence_(std::max<PropertyCommitSequence>(
          1, initial_commit_sequence)),
      buffers_(options.buffer_count) {
  if (!options_.enabled || options_.buffer_count < 2 ||
      options_.buffer_capacity_records == 0 ||
      options_.buffer_capacity_bytes == 0 || !flush_callback_) {
    throw std::invalid_argument("invalid property update manager options");
  }
  for (auto& buffer : buffers_) {
    buffer.records.reserve(std::min<std::size_t>(
        options_.buffer_capacity_records, 1'000'000));
  }
  active_buffer_ = 0;
  buffers_[active_buffer_].state = BufferState::kActive;
  worker_ = std::thread(&PropertyUpdateManager::WorkerMain, this);
}

PropertyUpdateManager::~PropertyUpdateManager() {
  std::string ignored;
  const bool flushed = FlushAndWait(&ignored);
  (void)flushed;
  StopWorker();
}

PropertyUpdateManager::PendingKey PropertyUpdateManager::MakePendingKey(
    const BufferedPropertyUpdate& update) const {
  return MakePendingKey(update.target.kind,
                        update.target.shard_id,
                        update.target.property_id,
                        update.record.src,
                        update.record.dst,
                        update.record.is_out,
                        update.record.edge_type);
}

PropertyUpdateManager::PendingKey PropertyUpdateManager::MakePendingKey(
    PropertyObjectKind kind,
    std::uint32_t shard_id,
    std::uint32_t property_id,
    VertexId_t src,
    VertexId_t dst,
    bool is_out,
    std::uint8_t edge_type) const {
  return PendingKey{kind, shard_id, property_id, src, dst, is_out, edge_type};
}

std::size_t PropertyUpdateManager::PendingBucketIndex(
    const PendingKey& key) const {
  return PendingKeyHash{}(key) % kPendingBucketCount;
}

void PropertyUpdateManager::PublishPending(
    const BufferedPropertyUpdate& update) {
  const PendingKey key = MakePendingKey(update);
  PendingBucket& bucket = pending_[PendingBucketIndex(key)];
  std::unique_lock<std::shared_mutex> lock(bucket.mutex);
  auto& value = bucket.values[key];
  if (value.commit_sequence <= update.record.commit_sequence) {
    value.base_sequence = update.record.base_sequence;
    value.commit_sequence = update.record.commit_sequence;
    value.value = update.record.value;
  }
}

void PropertyUpdateManager::RetirePending(
    const std::vector<BufferedPropertyUpdate>& records) {
  for (const auto& update : records) {
    const PendingKey key = MakePendingKey(update);
    PendingBucket& bucket = pending_[PendingBucketIndex(key)];
    std::unique_lock<std::shared_mutex> lock(bucket.mutex);
    const auto it = bucket.values.find(key);
    if (it != bucket.values.end() &&
        it->second.commit_sequence == update.record.commit_sequence) {
      bucket.values.erase(it);
    }
  }
}

std::size_t PropertyUpdateManager::EstimateBytes(
    const BufferedPropertyUpdate& update) const {
  return sizeof(BufferedPropertyUpdate) + update.record.value.size();
}

bool PropertyUpdateManager::EnsureActiveBuffer(
    std::unique_lock<std::mutex>* lock,
    std::string* error) {
  while (active_buffer_ == kNoBuffer && fatal_error_.empty() && !stop_) {
    blocked_submissions_.fetch_add(1, std::memory_order_relaxed);
    free_cv_.wait(*lock);
    for (std::size_t i = 0; i < buffers_.size(); ++i) {
      if (buffers_[i].state == BufferState::kFree) {
        buffers_[i].state = BufferState::kActive;
        active_buffer_ = i;
        break;
      }
    }
  }
  if (!fatal_error_.empty()) {
    SetError(error, fatal_error_);
    return false;
  }
  if (stop_) {
    SetError(error, "property update manager is stopping");
    return false;
  }
  return active_buffer_ != kNoBuffer;
}

void PropertyUpdateManager::QueueActiveBuffer() {
  if (active_buffer_ == kNoBuffer) return;
  Buffer& buffer = buffers_[active_buffer_];
  if (buffer.records.empty()) return;
  buffer.state = BufferState::kQueued;
  queued_buffers_.push_back(active_buffer_);
  active_buffer_ = kNoBuffer;
  for (std::size_t i = 0; i < buffers_.size(); ++i) {
    if (buffers_[i].state == BufferState::kFree) {
      buffers_[i].state = BufferState::kActive;
      active_buffer_ = i;
      break;
    }
  }
  worker_cv_.notify_one();
}

bool PropertyUpdateManager::Submit(
    BufferedPropertyUpdate update,
    PropertyCommitSequence* commit_sequence,
    std::string* error) {
  std::vector<BufferedPropertyUpdate> updates;
  updates.push_back(std::move(update));
  return SubmitBatch(std::move(updates), commit_sequence, error);
}

bool PropertyUpdateManager::SubmitBatch(
    std::vector<BufferedPropertyUpdate> updates,
    PropertyCommitSequence* commit_sequence,
    std::string* error) {
  if (updates.empty()) {
    SetError(error, "property update batch must not be empty");
    return false;
  }

  std::size_t batch_bytes = 0;
  for (const auto& update : updates) {
    const std::size_t record_bytes = EstimateBytes(update);
    if (record_bytes > options_.buffer_capacity_bytes - batch_bytes) {
      SetError(error, "property update batch exceeds buffer_capacity_bytes");
      return false;
    }
    batch_bytes += record_bytes;
  }
  if (updates.size() > options_.buffer_capacity_records) {
    SetError(error, "property update batch exceeds buffer_capacity_records");
    return false;
  }

  std::unique_lock<std::mutex> lock(mutex_);
  if (!EnsureActiveBuffer(&lock, error)) return false;
  Buffer* active = &buffers_[active_buffer_];
  if (!active->records.empty() &&
      (active->records.size() + updates.size() >
           options_.buffer_capacity_records ||
       active->bytes + batch_bytes > options_.buffer_capacity_bytes)) {
    QueueActiveBuffer();
    if (!EnsureActiveBuffer(&lock, error)) return false;
    active = &buffers_[active_buffer_];
  }

  const PropertyCommitSequence committed =
      next_commit_sequence_.fetch_add(1, std::memory_order_acq_rel);
  for (auto& update : updates) {
    update.record.commit_sequence = committed;
    PublishPending(update);
  }
  active->bytes += batch_bytes;
  active->records.insert(active->records.end(),
                         std::make_move_iterator(updates.begin()),
                         std::make_move_iterator(updates.end()));
  submitted_records_.fetch_add(updates.size(), std::memory_order_relaxed);
  submitted_bytes_.fetch_add(batch_bytes, std::memory_order_relaxed);

  if (active->records.size() >= options_.buffer_capacity_records ||
      active->bytes >= options_.buffer_capacity_bytes) {
    QueueActiveBuffer();
  }
  if (commit_sequence != nullptr) *commit_sequence = committed;
  return true;
}

bool PropertyUpdateManager::LookupLatest(
    PropertyObjectKind kind,
    std::uint32_t shard_id,
    std::uint32_t property_id,
    VertexId_t src,
    VertexId_t dst,
    bool is_out,
    std::uint8_t edge_type,
    std::string* value,
    PropertyCommitSequence* commit_sequence) const {
  if (value == nullptr) return false;
  const PendingKey key = MakePendingKey(kind, shard_id, property_id,
                                        src, dst, is_out, edge_type);
  const PendingBucket& bucket = pending_[PendingBucketIndex(key)];
  std::shared_lock<std::shared_mutex> lock(bucket.mutex);
  const auto it = bucket.values.find(key);
  if (it == bucket.values.end()) return false;
  *value = it->second.value;
  if (commit_sequence != nullptr) {
    *commit_sequence = it->second.commit_sequence;
  }
  return true;
}

bool PropertyUpdateManager::LookupExact(
    PropertyObjectKind kind,
    std::uint32_t shard_id,
    std::uint32_t property_id,
    VertexId_t src,
    VertexId_t dst,
    SequenceNumber_t base_sequence,
    bool is_out,
    std::uint8_t edge_type,
    std::string* value,
    PropertyCommitSequence* commit_sequence) const {
  if (value == nullptr) return false;
  const PendingKey key = MakePendingKey(kind, shard_id, property_id,
                                        src, dst, is_out, edge_type);
  const PendingBucket& bucket = pending_[PendingBucketIndex(key)];
  std::shared_lock<std::shared_mutex> lock(bucket.mutex);
  const auto it = bucket.values.find(key);
  if (it == bucket.values.end() ||
      it->second.base_sequence != base_sequence) {
    return false;
  }
  *value = it->second.value;
  if (commit_sequence != nullptr) {
    *commit_sequence = it->second.commit_sequence;
  }
  return true;
}

std::vector<BufferedPropertySnapshotEntry>
PropertyUpdateManager::SnapshotForSource(PropertyObjectKind kind,
                                         std::uint32_t shard_id,
                                         std::uint32_t property_id,
                                         VertexId_t src,
                                         bool is_out,
                                         std::uint8_t edge_type) const {
  std::vector<BufferedPropertySnapshotEntry> snapshot;
  for (const auto& bucket : pending_) {
    std::shared_lock<std::shared_mutex> lock(bucket.mutex);
    for (const auto& item : bucket.values) {
      const PendingKey& key = item.first;
      if (key.kind != kind || key.shard_id != shard_id ||
          key.property_id != property_id || key.src != src ||
          key.is_out != is_out || key.edge_type != edge_type) {
        continue;
      }
      snapshot.push_back(BufferedPropertySnapshotEntry{
          key.dst,
          item.second.base_sequence,
          item.second.commit_sequence,
          item.second.value});
    }
  }
  return snapshot;
}

bool PropertyUpdateManager::FlushBuffer(std::size_t buffer_index,
                                        std::string* error) {
  Buffer& buffer = buffers_[buffer_index];
  struct Group {
    bool target_is_persistent{false};
    std::vector<PropertyDeltaRecord> records;
  };
  std::unordered_map<PropertyDeltaTarget, Group, PropertyDeltaTargetHash>
      groups;
  for (const auto& update : buffer.records) {
    auto& group = groups[update.target];
    group.target_is_persistent =
        group.target_is_persistent || update.target_is_persistent;
    group.records.push_back(update.record);
  }
  for (auto& item : groups) {
    if (!flush_callback_(item.first,
                         std::move(item.second.records),
                         item.second.target_is_persistent,
                         error)) {
      return false;
    }
    delta_batches_published_.fetch_add(1, std::memory_order_relaxed);
  }
  RetirePending(buffer.records);
  return true;
}

void PropertyUpdateManager::WorkerMain() {
  while (true) {
    std::size_t buffer_index = kNoBuffer;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      worker_cv_.wait(lock, [&] { return stop_ || !queued_buffers_.empty(); });
      if (stop_ && queued_buffers_.empty()) break;
      buffer_index = queued_buffers_.front();
      queued_buffers_.pop_front();
      buffers_[buffer_index].state = BufferState::kFlushing;
      ++running_flushes_;
    }

    std::string error;
    const bool ok = FlushBuffer(buffer_index, &error);

    {
      std::lock_guard<std::mutex> lock(mutex_);
      --running_flushes_;
      Buffer& buffer = buffers_[buffer_index];
      if (ok) {
        buffer.records.clear();
        buffer.bytes = 0;
        buffer.state = BufferState::kFree;
        buffers_flushed_.fetch_add(1, std::memory_order_relaxed);
        if (active_buffer_ == kNoBuffer) {
          buffer.state = BufferState::kActive;
          active_buffer_ = buffer_index;
        }
      } else {
        buffer.state = BufferState::kFailed;
        if (fatal_error_.empty()) fatal_error_ = std::move(error);
        flush_failures_.fetch_add(1, std::memory_order_relaxed);
      }
      free_cv_.notify_all();
      if (queued_buffers_.empty() && running_flushes_ == 0) {
        idle_cv_.notify_all();
      }
    }
  }
}

bool PropertyUpdateManager::FlushAndWait(std::string* error) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!fatal_error_.empty()) {
    SetError(error, fatal_error_);
    return false;
  }
  QueueActiveBuffer();
  idle_cv_.wait(lock, [&] {
    return !fatal_error_.empty() ||
           (queued_buffers_.empty() && running_flushes_ == 0);
  });
  if (!fatal_error_.empty()) {
    SetError(error, fatal_error_);
    return false;
  }
  if (active_buffer_ == kNoBuffer) {
    for (std::size_t i = 0; i < buffers_.size(); ++i) {
      if (buffers_[i].state == BufferState::kFree) {
        buffers_[i].state = BufferState::kActive;
        active_buffer_ = i;
        break;
      }
    }
  }
  return true;
}

PropertyUpdateManagerStats PropertyUpdateManager::Stats() const {
  PropertyUpdateManagerStats stats;
  stats.submitted_records = submitted_records_.load(std::memory_order_relaxed);
  stats.submitted_bytes = submitted_bytes_.load(std::memory_order_relaxed);
  stats.buffers_flushed = buffers_flushed_.load(std::memory_order_relaxed);
  stats.delta_batches_published =
      delta_batches_published_.load(std::memory_order_relaxed);
  stats.flush_failures = flush_failures_.load(std::memory_order_relaxed);
  stats.blocked_submissions =
      blocked_submissions_.load(std::memory_order_relaxed);
  for (const auto& bucket : pending_) {
    std::shared_lock<std::shared_mutex> lock(bucket.mutex);
    stats.pending_keys += bucket.values.size();
  }
  return stats;
}

std::string PropertyUpdateManager::LastError() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return fatal_error_;
}

void PropertyUpdateManager::SetFatalError(std::string error) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (fatal_error_.empty()) fatal_error_ = std::move(error);
  free_cv_.notify_all();
  idle_cv_.notify_all();
}

void PropertyUpdateManager::StopWorker() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
  }
  worker_cv_.notify_all();
  free_cv_.notify_all();
  if (worker_.joinable()) worker_.join();
}

}  // namespace lsmgraph
